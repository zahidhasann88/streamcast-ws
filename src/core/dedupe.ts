/**
 * Bounded insertion-ordered Set for O(1) stream-ID and publisher-UID deduplication.
 * Evicts the oldest entry when capacity is reached.
 */
export class BoundedSet<T> {
  private readonly items = new Set<T>();
  private readonly capacity: number;

  constructor(capacity = 20_000) {
    if (capacity < 1) {
      throw new RangeError('BoundedSet capacity must be >= 1');
    }
    this.capacity = capacity;
  }

  has(value: T): boolean {
    return this.items.has(value);
  }

  /**
   * Adds `value` to the set. Returns `true` if `value` was newly added,
   * or `false` if it was already present.
   */
  add(value: T): boolean {
    if (this.items.has(value)) {
      return false;
    }
    if (this.items.size >= this.capacity) {
      const oldest = this.items.values().next();
      if (!oldest.done) {
        this.items.delete(oldest.value);
      }
    }
    this.items.add(value);
    return true;
  }

  delete(value: T): boolean {
    return this.items.delete(value);
  }

  get size(): number {
    return this.items.size;
  }

  clear(): void {
    this.items.clear();
  }
}
