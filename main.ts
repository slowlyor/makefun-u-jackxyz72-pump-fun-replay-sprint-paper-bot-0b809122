// Dawn Patrol — an early-launch tape bot for the pump.fun bonding-curve replay.
//
// Hello: the harness speaks agent-fun/replay/v2. Each batch arrives for one slot and the bot
// answers with one reply of orders for that slot. This bot watches every coin from its
// CreateEvent, reads its opening tape in batch orders per time slot, and buys only launches
// whose early tape looks like real crowd interest rather than a creator dump: the creator has
// not sold, a handful of distinct wallets are buying, buys outweigh sells, the price has not
// already run away, and net SOL is still flowing in. It exits on a stop loss, a trailing stop
// once a position is up (no fixed take-profit — a few coins run several multiples, so winners
// are left to run), a hold limit, and an instant exit when the creator sells, because on
// bonding curves the first minutes decide almost everything.
//
// Guardrail: the competition needs a round trip of at least 0.01 SOL on each of at least 20
// coins. If round trips lag the pace (few coins closed by mid-window), entry filters relax so
// the bot still clears the 20-coin bar instead of sitting on a handful of strict picks.
//
// Deterministic by construction: no clock, no randomness, no I/O; iteration only over
// insertion-ordered maps fed by the batch stream, so the two grading runs agree byte for byte.

import { runBot, type Batch, type BotState, type OrderInput, type Pubkey } from './vendor/makefun-bot.ts';

const LAMPORTS = 1_000_000_000n;
const CURVE_FEE_BPS = 125n; // protocol 95 + creator 30 bps, charged on every trade
const BPS = 10_000n;

/** SOL to aim for in one coin (each reply is still capped at 1% of its real SOL reserve). */
const STAKE_SOL = 0.08;
/** Smallest buy worth paying a transaction fee for. */
const MIN_BUY_SOL = 0.005;
/** Entry window: slots since CreateEvent. */
const MIN_AGE = 2;
const MAX_AGE = 30;
/** Real SOL reserve window at decision time. */
const MIN_RQ_SOL = 0.5;
const MAX_RQ_SOL = 40;
/** Creator's own buy in the create slot, at most. */
const MAX_DEV_BUY_SOL = 2;
/** Distinct non-creator buyers at entry: at least this, at most that. */
const MIN_BUYERS = 3;
const MAX_BUYERS = 10;
/** Total SOL bought by others before entry, at least. */
const MIN_BOUGHT_SOL = 0.4;
/** SOL sold by others, at most this share of SOL bought by others. */
const MAX_SELL_RATIO_BPS = 2000;
/** Price now over the create price, as a ratio window. */
const MIN_RUNUP = 1.03;
const MAX_RUNUP = 1.6;
/** Net SOL (buys minus sells) over the last FLOW_SLOTS slots, at least. */
const FLOW_SLOTS = 12;
const MIN_FLOW_SOL = 0.05;
/** Exits, as ratios of the position's cost. V2 tried letting winners run (no fixed
 *  take-profit, wide trail) and scored worse (-0.835 vs -0.334): on this window the tail
 *  does not pay, so the take-profit is back and the trail is tight. */
const TAKE_PROFIT = 1.18;
const STOP_LOSS = 0.88;
const TRAIL_ARM = 1.08;
const TRAIL_DROP = 0.92;
/** Exit after holding this many slots. */
const MAX_HOLD = 50;
/** Exit when no trade has hit the coin for this many slots. */
const STALE_SLOTS = 150;
/** Exit when net SOL over the fade window falls below this. */
const FADE_SLOTS = 15;
const FADE_NET_SOL = -0.5;
/** Relaxed entry filters once round trips lag the pace. Pace-based, not slot-based: the
 *  final window is 24h of block time, so the bar is 20 round trips by the end of it. The
 *  relaxed tier stays close to the strict one — v3 showed that a wide relaxation lets weak
 *  entries dominate the book. Coins already decided under the strict window are not revisited:
 *  relaxation only applies to coins still inside their entry window, so it must switch on
 *  early enough to matter. */
const RELAX_AFTER_SECS = 21_600; // 6h into the 24h window
const RELAXED_MAX_AGE = 30;
const RELAXED_MIN_BUYERS = 2;
const RELAXED_MIN_BOUGHT_SOL = 0.3;
const RELAXED_MAX_RUNUP = 2.0;

interface Tick {
  slot: number;
  buy: boolean;
  sol: number;
  user: Pubkey;
}

interface Coin {
  mint: Pubkey;
  createSlot: number;
  creator: Pubkey;
  vq0: bigint;
  vt0: bigint;
  devBuySol: number;
  devSold: boolean;
  trades: number;
  buyers: Set<Pubkey>;
  boughtSol: number;
  soldByOthersSol: number;
  recent: Tick[];
  lastTradeSlot: number;
  /** Decided for good: entered, or filtered out and never to be bought. */
  decided: boolean;
}

interface Held {
  openedSlot: number;
  peakValueSol: number;
  tookProfit: boolean;
}

/** What selling `tokens` against (vq, vt) would pay, curve fee deducted, in lamports. */
function sellValueLamports(tokens: bigint, vq: bigint, vt: bigint): bigint {
  if (tokens <= 0n || vt <= 0n) return 0n;
  const q = (tokens * vq) / (vt + tokens);
  return q - (q * CURVE_FEE_BPS + BPS - 1n) / BPS;
}

/** Net SOL (buys minus sells) over the last `w` slots of a coin's tape. */
function flowSol(c: Coin, now: number, w: number): number {
  let net = 0;
  for (let i = c.recent.length - 1; i >= 0; i--) {
    const t = c.recent[i]!;
    if (t.slot <= now - w) break;
    net += t.buy ? t.sol : -t.sol;
  }
  return net;
}

const coins = new Map<Pubkey, Coin>();
const held = new Map<Pubkey, Held>();
/** Mints with a sell in flight. */
const selling = new Set<Pubkey>();
/** Mints where a buy filled and a sell filled: the round trips that clear the guardrail. */
const roundTripped = new Set<Pubkey>();
const boughtOnce = new Set<Pubkey>();
let firstSlot = -1;
let firstTime: number | null = null;
let nowTime = 0;

function coinOf(mint: Pubkey): Coin {
  let c = coins.get(mint);
  if (c === undefined) {
    c = {
      mint,
      createSlot: -1,
      creator: '',
      vq0: 0n,
      vt0: 1n,
      devBuySol: 0,
      devSold: false,
      trades: 0,
      buyers: new Set(),
      boughtSol: 0,
      soldByOthersSol: 0,
      recent: [],
      lastTradeSlot: -1,
      decided: false,
    };
    coins.set(mint, c);
  }
  return c;
}

await runBot({
  onBatch(state: BotState, batch: Batch): OrderInput[] {
    const now = batch.slot;
    if (firstSlot < 0) firstSlot = now;
    for (const e of batch.events) {
      if (e.block_time === null) continue;
      if (firstTime === null) firstTime = e.block_time;
      if (e.block_time > nowTime) nowTime = e.block_time;
    }
    // Relaxed mode keeps the 20-coin guardrail reachable when strict picks are scarce.
    const relaxed =
      roundTripped.size < 20 && firstTime !== null && nowTime - firstTime > RELAX_AFTER_SECS;

    // Fold this batch's events into the tapes.
    for (const e of batch.events) {
      if (e.event === 'CreateEvent') {
        const c = coinOf(e.mint);
        c.createSlot = e.slot;
        c.creator = e.creator;
        c.vq0 = BigInt(e.virtual_quote_reserves);
        c.vt0 = BigInt(e.virtual_token_reserves);
        c.lastTradeSlot = e.slot;
      } else if (e.event === 'TradeEvent') {
        const c = coins.get(e.mint);
        if (c === undefined) continue;
        const sol = Number(e.sol_amount) / 1e9;
        c.trades += 1;
        c.lastTradeSlot = e.slot;
        c.recent.push({ slot: e.slot, buy: e.is_buy, sol, user: e.user });
        const isDev = e.user === c.creator;
        if (e.is_buy) {
          c.boughtSol += sol;
          if (isDev && e.slot === c.createSlot) c.devBuySol += sol;
          else if (!isDev) c.buyers.add(e.user);
        } else if (isDev) {
          c.devSold = true;
        } else {
          c.soldByOthersSol += sol;
        }
        // Prune ticks older than any window we read.
        const keepFrom = now - Math.max(FLOW_SLOTS, FADE_SLOTS) - 5;
        let cut = 0;
        while (cut < c.recent.length && c.recent[cut]!.slot <= keepFrom) cut++;
        if (cut > 0) c.recent.splice(0, cut);
      } else {
        const c = coins.get(e.mint);
        if (c !== undefined) c.decided = true;
      }
    }

    // Count round trips from this batch's fills.
    for (const f of batch.fills) {
      if (f.client_id.startsWith('_')) {
        held.delete(f.mint);
        selling.delete(f.mint);
        continue;
      }
      if (f.status !== 'filled') continue;
      if (f.side === 'buy') boughtOnce.add(f.mint);
      if (f.side === 'sell' && boughtOnce.has(f.mint)) roundTripped.add(f.mint);
    }
    for (const mint of [...selling]) {
      let stillPending = false;
      for (const p of state.pending.values()) if (p.order.mint === mint && p.order.side === 'sell') stillPending = true;
      if (!stillPending) selling.delete(mint);
    }
    for (const mint of [...held.keys()]) {
      if (!state.positions.has(mint) && !selling.has(mint)) {
        let buyPending = false;
        for (const p of state.pending.values()) if (p.order.mint === mint && p.order.side === 'buy') buyPending = true;
        if (!buyPending) held.delete(mint);
      }
    }

    const orders: OrderInput[] = [];
    const cap = state.hello.caps.max_orders_per_batch;
    const tx = state.txFeeLamports();
    const marks = new Map<Pubkey, { vq: bigint; vt: bigint }>();
    for (const m of batch.marks) marks.set(m.mint, { vq: BigInt(m.virtual_quote_reserves), vt: BigInt(m.virtual_token_reserves) });

    // Exits first: take profit, stop loss, trailing stop, hold limit, dev sell, stale, fade.
    for (const [mint, pos] of state.positions) {
      if (orders.length >= cap) break;
      if (selling.has(mint)) continue;
      const curve = state.curves.get(mint);
      if (curve === undefined || curve.complete) continue;
      let h = held.get(mint);
      if (h === undefined) {
        h = { openedSlot: now, peakValueSol: 0, tookProfit: false };
        held.set(mint, h);
      }
      const mk = marks.get(mint) ?? { vq: curve.virtual_quote_reserves, vt: curve.virtual_token_reserves };
      const valueSol = Number(sellValueLamports(pos.token_amount, mk.vq, mk.vt) - tx) / 1e9;
      const costSol = Number(pos.cost_lamports) / 1e9;
      if (valueSol > h.peakValueSol) h.peakValueSol = valueSol;
      const c = coins.get(mint);
      let exit = false;
      if (valueSol >= costSol * TAKE_PROFIT) exit = true;
      if (valueSol <= costSol * STOP_LOSS) exit = true;
      if (h.peakValueSol >= costSol * TRAIL_ARM && valueSol <= h.peakValueSol * TRAIL_DROP) exit = true;
      if (now - h.openedSlot >= MAX_HOLD) exit = true;
      if (c !== undefined) {
        if (c.devSold) exit = true;
        if (now - c.lastTradeSlot >= STALE_SLOTS) exit = true;
        if (flowSol(c, now, FADE_SLOTS) <= FADE_NET_SOL) exit = true;
      }
      if (!exit) continue;
      orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: 10_000 });
      selling.add(mint);
      if (c !== undefined) c.decided = true;
    }

    // Entries: one buy per coin, only while its opening tape still looks like a real crowd.
    let free = state.freeCashLamports();
    let openSlots = state.hello.caps.max_open_positions - state.committedPositionCount();
    for (const c of coins.values()) {
      if (orders.length >= cap) break;
      if (c.decided || c.createSlot < 0) continue;
      const curve = state.curves.get(c.mint);
      if (curve === undefined || curve.complete) continue;
      const age = now - c.createSlot;
      const maxAge = relaxed ? RELAXED_MAX_AGE : MAX_AGE;
      const minBuyers = relaxed ? RELAXED_MIN_BUYERS : MIN_BUYERS;
      const minBought = relaxed ? RELAXED_MIN_BOUGHT_SOL : MIN_BOUGHT_SOL;
      const maxRunup = relaxed ? RELAXED_MAX_RUNUP : MAX_RUNUP;
      if (age < MIN_AGE) continue;
      if (age > maxAge) {
        c.decided = true;
        continue;
      }
      let buyPending = false;
      for (const p of state.pending.values()) if (p.order.mint === c.mint && p.order.side === 'buy') buyPending = true;
      if (buyPending) continue;
      if (openSlots <= 0) continue;
      const rqSol = Number(curve.real_quote_reserves) / 1e9;
      if (rqSol < MIN_RQ_SOL || rqSol > MAX_RQ_SOL) continue;
      if (c.devSold || c.devBuySol > MAX_DEV_BUY_SOL) {
        c.decided = true;
        continue;
      }
      if (c.buyers.size < minBuyers || c.buyers.size > MAX_BUYERS) continue;
      if (c.boughtSol < minBought) continue;
      if (c.boughtSol > 0 && c.soldByOthersSol * 10_000 > c.boughtSol * MAX_SELL_RATIO_BPS) continue;
      if (c.vq0 <= 0n || c.vt0 <= 0n) continue;
      const runup = Number((curve.virtual_quote_reserves * c.vt0) / (curve.virtual_token_reserves * c.vq0));
      if (runup < MIN_RUNUP || runup > maxRunup) continue;
      if (flowSol(c, now, FLOW_SLOTS) < MIN_FLOW_SOL) continue;
      const capLamports = state.maxBuyLamports(c.mint);
      const stakeLamports = BigInt(Math.floor(STAKE_SOL * 1e9));
      const amount = capLamports < stakeLamports ? capLamports : stakeLamports;
      if (amount < BigInt(Math.floor(MIN_BUY_SOL * 1e9))) continue;
      if (free < amount + tx) continue;
      orders.push({ side: 'buy', mint: c.mint, quote_lamports: amount, max_slippage_bps: 10_000 });
      free -= amount + tx;
      openSlots -= 1;
      c.decided = true;
      held.set(c.mint, { openedSlot: now, peakValueSol: 0, tookProfit: false });
    }
    return orders;
  },
});
