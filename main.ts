// Momentum scalper for the pump.fun paper sprint (agent-fun/replay/v2).
//
// Wire: the harness sends a hello message (capital, caps, fees, latency in slots),
// then one batch per slot (events, marks, fills, account), then done. This bot
// answers every batch with a reply carrying its orders for that slot. One reply
// per batch, orders echo the batch seq through the shim.
//
// Strategy: ride fresh bonding-curve momentum. A curve becomes a buy once it is
// mature (past minAge slots since its CreateEvent: fresh curves are mostly
// bundled launches that get dumped) and its recent tape shows accelerating buy
// pressure over a short window: net SOL flowing in, several distinct buyers, a
// rising price sitting near its window high, and more net inflow in the newer
// half of the window than in the older half.
//
// The first buy is small. The position is then pyramided into strength only:
// each later batch adds while the price is at or above the last buy's and the
// position is still up, up to stakeSol. False starts cost little, runners get
// size. A position is sold whole on the first of a stop loss against cost, a
// trailing stop from its best close value, a fade in net flow, one big recorded
// sell, or a time limit. A curve that completes is closed by the replay.
//
// Guardrail probes keep the run above the minimum bar (20 coins, 0.01 SOL round
// trips): when the bot's own fills or coins lag a pace set in block time, it
// buys probeSol of a liquid curve and sells it as soon as the buy lands.
//
// Deterministic by construction: no clock, no randomness, iteration only over
// insertion-ordered maps fed by the batch stream. Two runs on the same data
// produce the same action log.

import { LAMPORTS_PER_SOL, runBot, type Batch, type BotState, type OrderInput, type Pubkey } from './vendor/makefun-bot.ts';

const LAMPORTS = 1e9;
// Curve fee on each side: protocol 95 bps + creator 30 bps fallback.
const CURVE_FEE = 0.0125;
const BPS = 10_000;

interface Params {
  /** Entry window in slots. */
  window: number;
  /** Net buy SOL (buys minus sells) over the window. */
  minNetSol: number;
  /** Buy trades in the window. */
  minBuys: number;
  /** Distinct buying wallets in the window. */
  minBuyers: number;
  /** Price now over price at the window start, minus 1. */
  minPriceUp: number;
  /** Upper bound on that rise: skip curves that already ran too far inside one window. */
  maxPriceUp: number;
  /** Newer-half net inflow must be at least this multiple of the older half's. */
  accel: number;
  /** Price now at least this share of the window's high. */
  nearHigh: number;
  /** Largest single wallet's share of the window's buy SOL. */
  maxBuyerShare: number;
  /** Curve real SOL bounds at entry. */
  minRqSol: number;
  maxRqSol: number;
  /** Slots since the CreateEvent before an entry is allowed. */
  minAge: number;
  /** SOL to put into one position in total. */
  stakeSol: number;
  /** Smallest buy worth sending. */
  minBuySol: number;
  /** Slippage on buys (bps); sells always go out at 10000. */
  buySlipBps: number;
  /** Exits. */
  stopLoss: number;
  trail: number;
  /** The trailing stop arms once value reached cost * (1 + trailArm). */
  trailArm: number;
  /** Exit when net SOL over the fade window falls below -fadeNetSol. */
  fadeWindow: number;
  fadeNetSol: number;
  /** Exit on any single recorded sell of at least this many SOL. */
  bigSellSol: number;
  /** Exit after this many slots held. */
  maxHold: number;
  /** Slots to wait after an exit before the same curve may be bought again. */
  cooldown: number;
  /** Most entries per curve over the run. */
  maxEntries: number;
  /** Keep adding to a position while the entry signal still holds, up to stakeSol. */
  pyramid: boolean;
  /** First buy of a position (SOL). */
  initialSol: number;
  /** Each add-on buy (SOL). */
  addSol: number;
  /** Add only once the price is this much above the last buy's decision price. */
  addStep: number;
  /** Add only while the position's close value is at least cost * (1 + addIfUp). */
  addIfUp: number;
  /** Add only while net SOL over the entry window is at least this. */
  addNetSol: number;
  /** Most momentum positions open at once (the rest of caps.max_open_positions is for probes). */
  maxMain: number;
  /** Guardrail pacing targets. */
  targetCoins: number;
  targetFills: number;
  paceSecs: number;
  probeSol: number;
  maxProbes: number;
  /** Most SOL at cost across all positions at once (a drawdown guard). */
  maxExposureSol: number;
}

const PARAMS: Params = {
  window: 40,
  minNetSol: 0.75,
  minBuys: 4,
  minBuyers: 4,
  minPriceUp: 0.06,
  maxPriceUp: 1.2,
  accel: 1,
  nearHigh: 0.9,
  maxBuyerShare: 0.6,
  minRqSol: 1.5,
  maxRqSol: 80,
  minAge: 240,
  stakeSol: 0.8,
  minBuySol: 0.01,
  buySlipBps: 1500,
  stopLoss: 0.18,
  trail: 0.25,
  trailArm: 0.06,
  fadeWindow: 20,
  fadeNetSol: 0.5,
  bigSellSol: 2,
  maxHold: 1000,
  cooldown: 120,
  maxEntries: 2,
  pyramid: true,
  initialSol: 0.08,
  addSol: 0.15,
  addStep: 0,
  addIfUp: 0,
  addNetSol: 0,
  maxMain: 4,
  targetCoins: 25,
  targetFills: 240,
  paceSecs: 21_600,
  probeSol: 0.01,
  maxProbes: 1,
  maxExposureSol: 5,
};

interface Trade {
  slot: number;
  buy: boolean;
  sol: number;
  user: string;
  price: number;
}

interface Tape {
  created: number | null;
  trades: Trade[];
  entries: number;
  lastExit: number;
}

interface Held {
  opened: number;
  peakValue: number;
  /** SOL sent in buys for this position (fees included). */
  invested: number;
  exiting: boolean;
  /** Some buy of this position filled. */
  filled: boolean;
  /** Recorded price when the last buy was decided. */
  lastAddPrice: number;
  /** Close value at the latest batch (SOL, curve and tx fees deducted). */
  lastValue: number;
}

const p = PARAMS;
const tapes = new Map<Pubkey, Tape>();
const held = new Map<Pubkey, Held>();
/** Probe round trips in flight, by mint: false while buying, true once selling. */
const probes = new Map<Pubkey, boolean>();
/** The bot's own filled orders and the coins they were on (harness closes excluded). */
let myFills = 0;
const myCoins = new Set<Pubkey>();
let firstTime: number | null = null;
let lastTime: number | null = null;
const keep = Math.max(p.window, p.fadeWindow) + 5;

const tapeOf = (mint: Pubkey): Tape => {
  let t = tapes.get(mint);
  if (t === undefined) {
    t = { created: null, trades: [], entries: 0, lastExit: -1e12 };
    tapes.set(mint, t);
  }
  return t;
};

/** Entry signal on a curve's tape at slot `now`. */
function entrySignal(t: Tape, now: number, rqSol: number): boolean {
  if (rqSol < p.minRqSol || rqSol > p.maxRqSol) return false;
  if (t.created === null || now - t.created < p.minAge) return false;
  const from = now - p.window;
  const mid = now - p.window / 2;
  let buySol = 0;
  let sellSol = 0;
  let buys = 0;
  let netOld = 0;
  let netNew = 0;
  let startPrice = -1;
  let high = 0;
  let last = 0;
  const byUser = new Map<string, number>();
  for (const tr of t.trades) {
    if (tr.slot <= from) {
      startPrice = tr.price;
      continue;
    }
    if (startPrice < 0) startPrice = tr.price;
    const signed = tr.buy ? tr.sol : -tr.sol;
    if (tr.slot <= mid) netOld += signed;
    else netNew += signed;
    if (tr.buy) {
      buySol += tr.sol;
      buys++;
      byUser.set(tr.user, (byUser.get(tr.user) ?? 0) + tr.sol);
    } else {
      sellSol += tr.sol;
    }
    if (tr.price > high) high = tr.price;
    last = tr.price;
  }
  if (buys < p.minBuys || byUser.size < p.minBuyers) return false;
  const net = buySol - sellSol;
  if (net < p.minNetSol) return false;
  if (startPrice <= 0) return false;
  const up = last / startPrice - 1;
  if (up < p.minPriceUp || up > p.maxPriceUp) return false;
  if (last < high * p.nearHigh) return false;
  if (netNew <= 0 || netNew < p.accel * netOld) return false;
  let top = 0;
  for (const v of byUser.values()) if (v > top) top = v;
  if (top > p.maxBuyerShare * buySol) return false;
  return true;
}

/** Net recorded SOL (buys minus sells) over the last `w` slots. */
function flow(t: Tape, now: number, w: number): number {
  let net = 0;
  for (let i = t.trades.length - 1; i >= 0; i--) {
    const tr = t.trades[i]!;
    if (tr.slot <= now - w) break;
    net += tr.buy ? tr.sol : -tr.sol;
  }
  return net;
}

/** SOL value of selling `tokens` into the curve at the given reserves, fees deducted. */
function sellValue(tokens: number, vq: number, vt: number, txSol: number): number {
  return ((tokens * vq) / (vt + tokens) / LAMPORTS) * (1 - CURVE_FEE) - txSol;
}

await runBot({
  onBatch(state: BotState, batch: Batch): OrderInput[] {
    const now = batch.slot;
    const bigSell = new Set<Pubkey>();
    const traded = new Set<Pubkey>();
    for (const f of batch.fills) {
      if (f.status === 'filled' && !f.client_id.startsWith('_')) {
        myFills++;
        myCoins.add(f.mint);
      }
    }
    for (const e of batch.events) {
      if (e.block_time !== null) {
        if (firstTime === null) firstTime = e.block_time;
        if (lastTime === null || e.block_time > lastTime) lastTime = e.block_time;
      }
      if (e.event === 'CreateEvent') {
        tapeOf(e.mint).created = e.slot;
      } else if (e.event === 'TradeEvent') {
        const t = tapeOf(e.mint);
        const sol = Number(e.sol_amount) / LAMPORTS;
        t.trades.push({
          slot: e.slot,
          buy: e.is_buy,
          sol,
          user: e.user,
          price: Number(e.virtual_quote_reserves) / Number(e.virtual_token_reserves),
        });
        traded.add(e.mint);
        if (!e.is_buy && sol >= p.bigSellSol) bigSell.add(e.mint);
      }
    }
    for (const mint of traded) {
      const t = tapes.get(mint)!;
      let cut = 0;
      while (cut < t.trades.length && t.trades[cut]!.slot <= now - keep) cut++;
      if (cut > 0) t.trades.splice(0, cut);
    }
    const marks = new Map(batch.marks.map((m) => [m.mint, m] as const));
    const pendingByMint = new Map<Pubkey, number>();
    for (const po of state.pending.values()) {
      pendingByMint.set(po.order.mint, (pendingByMint.get(po.order.mint) ?? 0) + 1);
    }

    const orders: OrderInput[] = [];
    const cap = state.hello.caps.max_orders_per_batch;
    const tx = Number(state.txFeeLamports()) / LAMPORTS;

    // Exits first.
    for (const [mint, h] of held) {
      const pos = state.positions.get(mint);
      const curve = state.curves.get(mint);
      if (pos === undefined) {
        if (!pendingByMint.has(mint)) {
          held.delete(mint);
          const t = tapeOf(mint);
          if (h.filled) t.lastExit = now;
          else t.entries--;
        }
        continue;
      }
      h.filled = true;
      if (curve === undefined || curve.complete) continue;
      const mk = marks.get(mint);
      const vq = mk ? Number(mk.virtual_quote_reserves) : Number(curve.virtual_quote_reserves);
      const vt = mk ? Number(mk.virtual_token_reserves) : Number(curve.virtual_token_reserves);
      const rqSol = (mk ? Number(mk.real_quote_reserves) : Number(curve.real_quote_reserves)) / LAMPORTS;
      const tokens = Number(pos.token_amount);
      const value = sellValue(tokens, vq, vt, tx);
      const cost = Number(pos.cost_lamports) / LAMPORTS;
      if (value > h.peakValue) h.peakValue = value;
      h.lastValue = value;
      let exit = h.exiting;
      const t = tapeOf(mint);
      if (!exit && value < cost * (1 - p.stopLoss)) exit = true;
      if (!exit && h.peakValue >= cost * (1 + p.trailArm) && value < h.peakValue * (1 - p.trail)) exit = true;
      if (!exit && flow(t, now, p.fadeWindow) < -p.fadeNetSol) exit = true;
      if (!exit && bigSell.has(mint)) exit = true;
      if (!exit && now - h.opened > p.maxHold && value < cost * (1 + p.holdIfUp)) exit = true;
      if (!exit) continue;
      h.exiting = true;
      if (pendingByMint.has(mint)) {
        let sellPending = false;
        for (const po of state.pending.values()) {
          if (po.order.mint === mint && po.order.side === 'sell') sellPending = true;
        }
        if (sellPending) continue;
      }
      if (orders.length >= cap) break;
      orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: 10_000 });
    }

    // Probe round trips: sell the whole probe as soon as its buy has landed.
    for (const [mint, selling] of probes) {
      const pos = state.positions.get(mint);
      const curve = state.curves.get(mint);
      if (pos === undefined) {
        if (!pendingByMint.has(mint)) probes.delete(mint);
        continue;
      }
      if (curve === undefined || curve.complete || pendingByMint.has(mint)) continue;
      if (orders.length >= cap) break;
      orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: 10_000 });
      if (!selling) probes.set(mint, true);
    }

    // Then entries and adds.
    let free = Number(state.freeCashLamports()) / LAMPORTS;
    let slotsLeft = state.hello.caps.max_open_positions - state.committedPositionCount();
    let mainLeft = p.maxMain - held.size;
    let exposure = 0;
    for (const [mint, h] of held) {
      exposure += Math.max(h.invested, Number(state.positions.get(mint)?.cost_lamports ?? 0n) / LAMPORTS);
    }
    for (const mint of traded) {
      if (orders.length >= cap) break;
      const curve = state.curves.get(mint);
      if (curve === undefined || curve.complete) continue;
      if (pendingByMint.has(mint) || probes.has(mint)) continue;
      const t = tapes.get(mint)!;
      const h = held.get(mint);
      const price = Number(curve.virtual_quote_reserves) / Number(curve.virtual_token_reserves);
      const rqSol = Number(curve.real_quote_reserves) / LAMPORTS;
      let want: number;
      if (h === undefined) {
        if (slotsLeft <= 0 || mainLeft <= 0) continue;
        if (t.entries >= p.maxEntries || now - t.lastExit < p.cooldown) continue;
        if (!entrySignal(t, now, rqSol)) continue;
        want = Math.min(p.initialSol, p.stakeSol);
      } else {
        // Pyramid into strength only: the position is up, the price stepped up from
        // the last add, and net SOL still flows in.
        if (!p.pyramid || h.exiting || !h.filled || h.invested >= p.stakeSol - p.minBuySol) continue;
        const pos = state.positions.get(mint);
        if (pos === undefined) continue;
        if (price < h.lastAddPrice * (1 + p.addStep)) continue;
        if (h.lastValue < (Number(pos.cost_lamports) / LAMPORTS) * (1 + p.addIfUp)) continue;
        if (flow(t, now, p.window) < p.addNetSol) continue;
        if (rqSol > p.maxRqSol) continue;
        want = Math.min(p.addSol, p.stakeSol - h.invested);
      }
      const capSol = Math.floor(Number(state.maxBuyLamports(mint))) / LAMPORTS;
      let amount = Math.min(capSol, want, p.maxExposureSol - exposure);
      if (amount > free - tx) amount = free - tx;
      if (amount < p.minBuySol) continue;
      const lamports = BigInt(Math.floor(amount * LAMPORTS));
      orders.push({ side: 'buy', mint, quote_lamports: lamports, max_slippage_bps: p.buySlipBps });
      free -= amount + tx;
      exposure += amount;
      if (h === undefined) {
        held.set(mint, {
          opened: now,
          peakValue: 0,
          invested: amount,
          exiting: false,
          filled: false,
          lastAddPrice: price,
          lastValue: 0,
        });
        t.entries++;
        slotsLeft--;
        mainLeft--;
      } else {
        h.invested += amount;
        h.lastAddPrice = price;
      }
    }

    // Guardrail pacing: open a probe when the bot's fills or coins lag the pace.
    if (probes.size < p.maxProbes && slotsLeft > 0 && firstTime !== null && lastTime !== null) {
      const pace = Math.min(1, (lastTime - firstTime) / p.paceSecs);
      const needCoins = myCoins.size < Math.ceil(p.targetCoins * pace);
      const needFills = myFills < Math.ceil(p.targetFills * pace);
      if (needCoins || needFills) {
        const minRq = (p.probeSol * BPS) / state.hello.caps.max_order_bps_of_real_reserve;
        for (const mint of traded) {
          if (orders.length >= cap || probes.size >= p.maxProbes) break;
          if (held.has(mint) || probes.has(mint) || pendingByMint.has(mint) || state.positions.has(mint)) continue;
          if (needCoins && !needFills && myCoins.has(mint)) continue;
          const curve = state.curves.get(mint);
          if (curve === undefined || curve.complete) continue;
          if (Number(curve.real_quote_reserves) / LAMPORTS < minRq * 1.5) continue;
          if (free < p.probeSol + 2 * tx) break;
          orders.push({
            side: 'buy',
            mint,
            quote_lamports: BigInt(Math.floor(p.probeSol * LAMPORTS)),
            max_slippage_bps: p.buySlipBps,
          });
          probes.set(mint, false);
          free -= p.probeSol + tx;
        }
      }
    }
    return orders;
  },
});
