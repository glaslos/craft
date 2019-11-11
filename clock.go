package craft

// getClockUncertainty returns the current clock uncertainty, i.e., max possible clock offsets
// between any two clocks in the network, in the exponent of 10 nanoseconds.
// For example, a return value of 5 means the current uncertainty is 10 5ns = 100us
func getClockUncertainty() int {
	return 5
}
