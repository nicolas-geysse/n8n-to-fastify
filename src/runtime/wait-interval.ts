const unitMilliseconds = {
  seconds: 1_000,
  minutes: 60_000,
  hours: 3_600_000,
  days: 86_400_000,
} as const;

const maxTimerDelay = 2_147_483_647;

export function getWaitDelayMs(amount: unknown, unit: unknown): number {
  if (typeof unit !== 'string' || !Object.hasOwn(unitMilliseconds, unit)) {
    throw new Error('Wait unit must be seconds, minutes, hours, or days');
  }
  const numericAmount = typeof amount === 'string' && amount.trim() ? Number(amount) : amount;
  if (typeof numericAmount !== 'number' || !Number.isFinite(numericAmount) || numericAmount < 0) {
    throw new Error('Wait amount must be a finite nonnegative number');
  }
  const delay = numericAmount * unitMilliseconds[unit as keyof typeof unitMilliseconds];
  if (delay > Number.MAX_SAFE_INTEGER) throw new Error('Wait interval is too large');
  return delay;
}

export async function waitForInterval(amount: unknown, unit: unknown): Promise<void> {
  let remaining = getWaitDelayMs(amount, unit);
  while (remaining > maxTimerDelay) {
    await new Promise<void>(resolve => setTimeout(resolve, maxTimerDelay));
    remaining -= maxTimerDelay;
  }
  await new Promise<void>(resolve => setTimeout(resolve, remaining));
}
