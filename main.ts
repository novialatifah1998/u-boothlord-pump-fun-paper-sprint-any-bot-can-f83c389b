// Launch sniper for the pump.fun paper sprint (agent-fun/replay/v2).
//
// Wire: the harness sends a hello message (capital, caps, fees, latency in slots),
// then one batch per slot (events, marks, fills, account), then done. This bot
// answers every batch with a reply carrying its orders for that slot. One reply
// per batch, orders echo the batch seq through the shim.
//
// Strategy: snipe launches, don't chase runners. Every coin is watched from its
// CreateEvent. In its first few slots the bot reads the opening tape: how big the
// creator's own buy was, how many other wallets piled in with it (a launch
// bundled by many wallets in its first slot is usually the creator's cluster
// waiting to dump), whether the creator has already sold, and how much real SOL
// the curve holds. It buys only launches that pass the filter, takes profit
// fast, cuts losers fast, never holds past a short time limit, and leaves at
// once when the creator sells. Small stakes, many coins: a single rug costs
// little, and the guardrail (20 coins with a 0.01 SOL round trip) is cleared
// by the volume of launches alone.
//
// Money math is in SOL floats. Price is virtual quote reserves over virtual
// token reserves. Selling T tokens into the curve pays T * vq / (vt + T) SOL
// before the 1.25% curve fee and the per-order transaction fee.
//
// Deterministic by construction: no clock, no randomness, iteration only over
// insertion-ordered maps fed by the batch stream. Two runs on the same data
// produce the same action log.

import { LAMPORTS_PER_SOL, runBot, type Batch, type BotState, type OrderInput, type Pubkey } from './vendor/makefun-bot.ts';

const LAMPORTS = 1e9;
const CURVE_FEE = 0.0125;

interface Params {
  /** Youngest age (slots since create) at which a coin may be bought. */
  minAge: number;
  /** Oldest age at which a first buy may still be decided. */
  maxAge: number;
  /** SOL to aim for per coin in total. */
  stakeSol: number;
  /** Smallest single buy worth paying the tx fee for. */
  minBuySol: number;
  /** Real SOL reserve window at decision time. */
  minRealSol: number;
  maxRealSol: number;
  /** Creator's own buys in the create slot: SOL window. */
  minDevBuy: number;
  maxDevBuy: number;
  /** Distinct (non-creator) buyers seen before entry, at most: a launch bundled by many wallets is a dump in waiting. */
  maxBuyers: number;
  /** Lamports sold by others / lamports bought, at most (0..1). */
  maxSellRatio: number;
  /** Largest single non-creator sell seen, as a share of the real SOL reserve before it, at most. */
  maxBigSellShare: number;
  /** Price now over the create price: window. */
  minRunup: number;
  maxRunup: number;
  /** The recent-flow window (slots) and what it must show. */
  flowWindow: number;
  minFlowSol: number;
  /** Creator sold before entry: skip the coin. */
  skipDevSold: boolean;
  /** Slippage allowed on buys (bps). */
  buySlipBps: number;
  /** First take-profit: sell tpPart of the position once it is worth tp1 over its cost. */
  tp1: number;
  tp1Part: number;
  /** After the first take-profit, the rest stops out at this much under its cost (negative = above). */
  slAfterTp1: number;
  /** Exit when the position is worth this much over its cost. */
  tp: number;
  /** Exit when it is worth this much under its cost. */
  sl: number;
  /** Trailing stop: once up at least trailArm, exit on a fall of trail from the best value. */
  trailArm: number;
  trail: number;
  /** Exit after holding this many slots. */
  maxHold: number;
  /** Exit when the creator sells while held. */
  exitOnDevSell: boolean;
  /** Exit when no trade has hit the coin for this many slots. */
  staleSlots: number;
}

const P: Params = {
  minAge: 0,
  maxAge: 3,
  stakeSol: 0.1,
  minBuySol: 0.005,
  minRealSol: 0.3,
  maxRealSol: 30,
  minDevBuy: 0,
  maxDevBuy: 2.5,
  maxBuyers: 3,
  maxSellRatio: 1,
  maxBigSellShare: 1,
  minRunup: 0,
  maxRunup: 2,
  flowWindow: 10,
  minFlowSol: -1e9,
  skipDevSold: true,
  buySlipBps: 10_000,
  tp1: 0.08,
  tp1Part: 0.5,
  slAfterTp1: 0,
  tp: 0.12,
  sl: 0.1,
  trailArm: 0.1,
  trail: 0.08,
  maxHold: 12,
  exitOnDevSell: true,
  staleSlots: 1e9,
};

interface Tick {
  slot: number;
  isBuy: boolean;
  sol: number;
  user: string;
}

interface Coin {
  mint: Pubkey;
  createSlot: number;
  creator: Pubkey;
  /** Create-time reserves: the price run-up is measured against them. */
  vq0: number;
  vt0: number;
  devBuy: number;
  trades: number;
  buyers: Set<Pubkey>;
  bought: number;
  soldByOthers: number;
  bigSellShare: number;
  devSold: boolean;
  lastTradeSlot: number;
  /** Trades of the last flowWindow slots (older ones dropped as new ones come). */
  recent: Tick[];
  buysSent: number;
  /** Decision price (vq/vt as a pair) of the last buy sent. */
  lastBuyVq: number;
  lastBuyVt: number;
  /** Never buy it again (exited, filtered out for good, or completed). */
  done: boolean;
  complete: boolean;
}

interface Held {
  openedSlot: number;
  txPaidSol: number;
  best: number;
  devSoldAtOpen: boolean;
  tookTp1: boolean;
}

const coins = new Map<Pubkey, Coin>();
const held = new Map<Pubkey, Held>();
/** Mints with a sell in flight. */
const selling = new Set<Pubkey>();

/** What selling `tokens` against (vq, vt) would pay after the curve fee. */
function sellValue(tokens: number, vq: number, vt: number): number {
  if (tokens <= 0 || vt <= 0) return 0;
  return ((tokens * vq) / (vt + tokens) / LAMPORTS) * (1 - CURVE_FEE);
}

/** Net SOL bought minus sold and the distinct buyers, over the last flowWindow slots. */
function flow(c: Coin, slot: number): { net: number; buyers: number } {
  const from = slot - P.flowWindow;
  let drop = 0;
  while (drop < c.recent.length && c.recent[drop]!.slot <= from) drop++;
  if (drop > 0) c.recent.splice(0, drop);
  let net = 0;
  const buyers = new Set<Pubkey>();
  for (const t of c.recent) {
    if (t.isBuy) {
      net += t.sol;
      if (t.user !== c.creator) buyers.add(t.user);
    } else {
      net -= t.sol;
    }
  }
  return { net, buyers: buyers.size };
}

function hasPendingBuy(state: BotState, mint: Pubkey): boolean {
  for (const po of state.pending.values()) {
    if (po.order.mint === mint && po.order.side === 'buy') return true;
  }
  return false;
}

function hasPendingSell(state: BotState, mint: Pubkey): boolean {
  for (const po of state.pending.values()) {
    if (po.order.mint === mint && po.order.side === 'sell') return true;
  }
  return false;
}

await runBot({
  onBatch(state: BotState, batch: Batch): OrderInput[] {
    // Fold this batch's events into the per-coin tapes.
    for (const e of batch.events) {
      if (e.event === 'CreateEvent') {
        coins.set(e.mint, {
          mint: e.mint,
          createSlot: e.slot,
          creator: e.creator,
          vq0: Number(e.virtual_quote_reserves),
          vt0: Number(e.virtual_token_reserves),
          devBuy: 0,
          trades: 0,
          buyers: new Set(),
          bought: 0,
          soldByOthers: 0,
          bigSellShare: 0,
          devSold: false,
          lastTradeSlot: e.slot,
          recent: [],
          buysSent: 0,
          lastBuyVq: 0,
          lastBuyVt: 1,
          done: false,
          complete: false,
        });
      } else if (e.event === 'TradeEvent') {
        const c = coins.get(e.mint);
        if (c === undefined) continue;
        const sol = Number(e.sol_amount) / LAMPORTS;
        c.trades += 1;
        c.lastTradeSlot = e.slot;
        c.recent.push({ slot: e.slot, isBuy: e.is_buy, sol, user: e.user });
        const isDev = e.user === c.creator;
        if (e.is_buy) {
          c.bought += sol;
          if (isDev && e.slot === c.createSlot) c.devBuy += sol;
          else if (!isDev) c.buyers.add(e.user);
        } else if (isDev) {
          c.devSold = true;
        } else {
          c.soldByOthers += sol;
          const rqBefore = Number(e.real_quote_reserves) / LAMPORTS + sol;
          if (rqBefore > 0) {
            const share = sol / rqBefore;
            if (share > c.bigSellShare) c.bigSellShare = share;
          }
        }
      } else {
        const c = coins.get(e.mint);
        if (c !== undefined) {
          c.complete = true;
          c.done = true;
        }
      }
    }

    const orders: OrderInput[] = [];
    const cap = state.hello.caps.max_orders_per_batch;
    const tx = Number(state.txFeeLamports()) / LAMPORTS;
    const marks = new Map<Pubkey, { vq: number; vt: number }>();
    for (const m of batch.marks) {
      marks.set(m.mint, { vq: Number(m.virtual_quote_reserves), vt: Number(m.virtual_token_reserves) });
    }

    // Settle fills: count them, track tx fees paid, open Held records on buy fills.
    for (const f of batch.fills) {
      if (f.client_id.startsWith('_')) {
        held.delete(f.mint);
        selling.delete(f.mint);
        continue;
      }
      const h = held.get(f.mint);
      if (f.status !== 'rejected' && h !== undefined) {
        h.txPaidSol += Number(f.tx_fee_lamports) / LAMPORTS;
      }
      if (f.side === 'buy' && f.status === 'filled' && h === undefined) {
        held.set(f.mint, {
          openedSlot: f.slot,
          txPaidSol: Number(f.tx_fee_lamports) / LAMPORTS,
          best: 0,
          devSoldAtOpen: coins.get(f.mint)?.devSold ?? false,
          tookTp1: false,
        });
      }
    }
    for (const mint of [...selling]) {
      if (!hasPendingSell(state, mint)) selling.delete(mint);
    }
    for (const mint of [...held.keys()]) {
      if (!state.positions.has(mint) && !selling.has(mint) && !hasPendingBuy(state, mint)) {
        held.delete(mint);
      }
    }

    // Exits first.
    for (const [mint, pos] of state.positions) {
      if (orders.length >= cap) break;
      if (selling.has(mint)) continue;
      const c = coins.get(mint);
      let h = held.get(mint);
      if (h === undefined) {
        h = { openedSlot: state.slot, txPaidSol: tx, best: 0, devSoldAtOpen: c?.devSold ?? false, tookTp1: false };
        held.set(mint, h);
      }
      let mk = marks.get(mint);
      if (mk === undefined) {
        const cv = state.curves.get(mint);
        if (cv === undefined) continue;
        mk = { vq: Number(cv.virtual_quote_reserves), vt: Number(cv.virtual_token_reserves) };
      }
      const tokens = Number(pos.token_amount);
      const value = sellValue(tokens, mk.vq, mk.vt) - tx;
      const basis = Number(pos.cost_lamports) / LAMPORTS + h.txPaidSol;
      if (value > h.best) h.best = value;
      let exit = false;
      if (basis > 0) {
        // First take-profit: sell half once up tp1, the rest rides with a stop at cost.
        if (!h.tookTp1 && value >= basis * (1 + P.tp1)) {
          const part = Math.floor(tokens * P.tp1Part);
          if (part > 0 && part < tokens) {
            orders.push({ side: 'sell', mint, token_amount: BigInt(part), max_slippage_bps: 10_000 });
            selling.add(mint);
            h.tookTp1 = true;
            h.best = 0;
            if (c !== undefined) c.done = true;
            continue;
          }
        }
        if (value >= basis * (1 + P.tp)) exit = true;
        const sl = h.tookTp1 ? P.slAfterTp1 : P.sl;
        if (value <= basis * (1 - sl)) exit = true;
        if (h.best >= basis * (1 + P.trailArm) && value <= h.best * (1 - P.trail)) exit = true;
      }
      if (state.slot - h.openedSlot >= P.maxHold) exit = true;
      if (c !== undefined) {
        if (P.exitOnDevSell && c.devSold && !h.devSoldAtOpen) exit = true;
        if (state.slot - c.lastTradeSlot >= P.staleSlots) exit = true;
        if (flow(c, state.slot).net < -1e9) exit = true; // exitFlow disabled (threshold below any real flow)
      }
      if (!exit) continue;
      orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: 10_000 });
      selling.add(mint);
      if (c !== undefined) c.done = true;
    }

    // Entries: launches in their first slots that pass the rug filter.
    let free = Number(state.freeCashLamports()) / LAMPORTS - tx * orders.length;
    let open = state.committedPositionCount();
    for (const c of coins.values()) {
      if (orders.length >= cap) break;
      if (c.done || c.complete) continue;
      const age = state.slot - c.createSlot;
      if (age > P.maxAge) {
        c.done = true;
        continue;
      }
      if (age < P.minAge) continue;
      if (c.buysSent > 0) continue; // one buy per coin
      if (open >= state.hello.caps.max_open_positions) continue;
      const curve = state.curves.get(c.mint);
      if (curve === undefined || curve.complete) continue;
      if (c.devSold && P.skipDevSold) {
        c.done = true;
        continue;
      }
      // Rug filter.
      const rq = Number(curve.real_quote_reserves) / LAMPORTS;
      if (rq < P.minRealSol || rq > P.maxRealSol) continue;
      if (c.devBuy < P.minDevBuy || c.devBuy > P.maxDevBuy) continue;
      if (c.buyers.size > P.maxBuyers) continue;
      if (c.bought > 0 && c.soldByOthers / c.bought > P.maxSellRatio) continue;
      if (c.bigSellShare > P.maxBigSellShare) continue;
      const runup = (Number(curve.virtual_quote_reserves) * c.vt0) / (Number(curve.virtual_token_reserves) * c.vq0);
      if (runup > P.maxRunup || runup < P.minRunup) continue;
      const fl = flow(c, state.slot);
      if (fl.net < P.minFlowSol) continue;
      const maxBuy = Number(state.maxBuyLamports(c.mint)) / LAMPORTS;
      const amount = Math.min(P.stakeSol, maxBuy);
      if (amount < P.minBuySol) continue;
      if (free < amount + tx) continue;
      orders.push({
        side: 'buy',
        mint: c.mint,
        quote_lamports: BigInt(Math.floor(amount * LAMPORTS)),
        max_slippage_bps: P.buySlipBps,
      });
      free -= amount + tx;
      c.buysSent += 1;
      open += 1;
    }
    return orders;
  },
});
