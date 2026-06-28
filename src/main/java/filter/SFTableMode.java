package filter;

/**
 * why we choose below four setting for shrink filter mode?
 * because we want to ensure the size of an entry is 32 bits
 * then we can store four entries in two long numbers
 */
public enum SFTableMode {
    _8_12_12, _12_10_10, _16_8_8
}
