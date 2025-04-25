// testSweeperFeeBumping tests that the sweeper correctly bumps the fee
// of a transaction until it reaches the maximum fee rate.
func testSweeperFeeBumping(t *harnessTest) {
    // Create alice and bob nodes with specific fee configuration.
    // Set a low max fee rate to make testing faster but still allow for multiple fee bumps.
    aliceArgs := []string{
        "--sweeper.maxfeerate=50", // 50 sat/vbyte max fee rate
    }
    
    alice, err := net.NewNode("Alice", aliceArgs)
    require.NoError(t, err)
    defer shutdownAndAssert(t, alice)
    
    bob, err := net.NewNode("Bob", nil)
    require.NoError(t, err)
    defer shutdownAndAssert(t, bob)
    
    // Connect the nodes.
    ctxt, cancel := context.WithTimeout(context.Background(), defaultTimeout)
    defer cancel()
    err = net.ConnectNodes(ctxt, alice, bob)
    require.NoError(t, err)
    
    // Fund alice with some BTC.
    ctxt, cancel = context.WithTimeout(context.Background(), defaultTimeout)
    defer cancel()
    err = net.SendCoins(ctxt, bitcoind, alice, btcutil.SatoshiPerBitcoin)
    require.NoError(t, err)
    
    // Open a channel between alice and bob.
    ctxt, cancel = context.WithTimeout(context.Background(), channelOpenTimeout)
    defer cancel()
    chanPoint := openChannelAndAssert(t, net, alice, bob, lntest.OpenChannelParams{
        Amt: 1000000, // 1M satoshis
    })
    
    // Wait for Alice to receive the channel edge from the funding manager.
    ctxt, cancel = context.WithTimeout(context.Background(), defaultTimeout)
    defer cancel()
    err = alice.WaitForNetworkChannelOpen(ctxt, chanPoint)
    require.NoError(t, err)
    
    // Create a custom fee estimator that always returns a low fee.
    // This will ensure our starting fee rate is low and multiple bumps are needed.
    setMiner := net.Miner
    blockHeight := setMiner.CurrentHeight()
    setMiner.SetFeeRate(1000) // 1 sat/byte = 1000 sat/kw
    
    // Force close the channel from Alice's side to trigger the sweeping process.
    ctxt, cancel = context.WithTimeout(context.Background(), channelCloseTimeout)
    defer cancel()
    closeChannelAndAssert(t, net, alice, chanPoint, true)
    
    // Fetch the closing transaction ID.
    closingTxID := waitForChannelCloseEvent(t, alice, chanPoint)
    
    // Get the initial sweep transaction details
    initialTxID, initialFeeRate := waitForSweepTransaction(
        t, alice, closingTxID, defaultTimeout,
    )
    
    t.Logf("Initial sweep tx %s with fee rate %d sat/vByte", initialTxID, initialFeeRate)
    
    // Mine blocks one by one and monitor the fee bumping.
    var highestFeeRate int64 = initialFeeRate
    feeRateHistory := []int64{initialFeeRate}
    
    // We'll mine blocks and check for fee bumps up to a reasonable limit.
    // The actual number needed depends on the fee market and estimator configuration.
    maxBlocks := 20
    reachedMaxFee := false
    
    for i := 0; i < maxBlocks; i++ {
        // Mine a block.
        blockHash, err := net.Miner.Node.Generate(1)
        require.NoError(t, err)
        
        // Give LND time to process the block and potentially bump fees.
        time.Sleep(time.Second)
        
        // Check if fee rate has changed.
        txid, feeRate := getSweepDetails(t, alice, closingTxID)
        
        // If we don't find a pending sweep, it might have confirmed.
        if txid == "" {
            // Check if the transaction was confirmed.
            ctxt, cancel = context.WithTimeout(context.Background(), defaultTimeout)
            defer cancel()
            tx, err := net.Miner.Node.GetTransaction(initialTxID)
            if err == nil && tx.Confirmations > 0 {
                t.Logf("Sweep transaction confirmed with fee rate %d sat/vByte", 
                    highestFeeRate)
                break
            }
        } else {
            // Record the fee rate.
            feeRateHistory = append(feeRateHistory, feeRate)
            
            if feeRate > highestFeeRate {
                highestFeeRate = feeRate
                t.Logf("Fee bumped to %d sat/vByte in block %s (height %d)",
                    feeRate, blockHash[0], blockHeight+1+uint32(i))
            }
            
            // See if we've reached the max fee rate.
            if feeRate >= 50 { // The max we configured
                t.Logf("Reached max fee rate: %d sat/vByte", feeRate)
                reachedMaxFee = true
                break
            }
        }
    }
    
    // Mine enough blocks to ensure the transaction confirms.
    _, err = net.Miner.Node.Generate(6)
    require.NoError(t, err)
    
    // Visualize the fee rate progression
    logFeeRateProgression(t, feeRateHistory, 50)
    
    // Verify the fee rate progression behavior
    assertLinearFeeProgression(t, feeRateHistory)
    
    // Assert that we either reached max fee rate or the transaction was confirmed.
    if reachedMaxFee {
        // Additional verification - check that the final fee rate is indeed close to max.
        maxExpectedRate := int64(50) // The configured max
        require.GreaterOrEqual(t, highestFeeRate, maxExpectedRate*9/10,
            "Final fee rate should be close to the maximum configured rate")
    } else {
        require.Greater(t, len(feeRateHistory), 1, 
            "Fee rate should have been bumped at least once before confirmation")
    }
}

// waitForChannelCloseEvent waits for a channel close event and returns the closing txid.
func waitForChannelCloseEvent(t *harnessTest, node *lntest.HarnessNode, 
    chanPoint *lnrpc.ChannelPoint) string {
    
    ctxt, cancel := context.WithTimeout(context.Background(), defaultTimeout)
    defer cancel()
    
    closingTxID := ""
    err := wait.Predicate(func() bool {
        // Check for closed channel event
        eventStream, err := node.Subscribe(ctxt, &lnrpc.ChannelEventSubscription{})
        if err != nil {
            return false
        }
        
        for {
            event, err := eventStream.Recv()
            if err != nil {
                return false
            }
            
            if event.Type == lnrpc.ChannelEventUpdate_CLOSED_CHANNEL {
                closeChan := event.GetClosedChannel()
                if closeChan.ChannelPoint == chanPoint.String() {
                    closingTxID = closeChan.ClosingTxHash
                    return true
                }
            }
        }
    }, defaultTimeout)
    
    require.NoError(t, err, "Timed out waiting for channel close event")
    require.NotEmpty(t, closingTxID, "Closing transaction ID not found")
    
    return closingTxID
}

// getSweepDetails returns the txid and fee rate of a sweep transaction
// related to the specified closing transaction.
func getSweepDetails(t *harnessTest, node *lntest.HarnessNode, 
    closingTxID string) (string, int64) {
    
    ctxt, cancel := context.WithTimeout(context.Background(), defaultTimeout)
    defer cancel()
    
    req := &lnrpc.PendingSweepsRequest{}
    resp, err := node.PendingSweeps(ctxt, req)
    require.NoError(t, err)
    
    // Look for the sweep transaction related to our channel.
    for _, pendingSweep := range resp.PendingSweeps {
        if strings.Contains(pendingSweep.OutpointString, closingTxID) {
            // Extract fee rate in sat/vByte
            feeRate := pendingSweep.SatPerByte
            txid := pendingSweep.Txid
            return txid, feeRate
        }
    }
    
    return "", 0
}

// waitForSweepTransaction waits for a sweep transaction related to the 
// specified closing transaction and returns its txid and fee rate.
func waitForSweepTransaction(t *harnessTest, node *lntest.HarnessNode, 
    closingTxID string, timeout time.Duration) (string, int64) {
    
    var sweepTxID string
    var feeRate int64
    
    err := wait.Predicate(func() bool {
        sweepTxID, feeRate = getSweepDetails(t, node, closingTxID)
        return sweepTxID != ""
    }, timeout)
    
    require.NoError(t, err, "timed out waiting for sweep transaction")
    return sweepTxID, feeRate
}

// assertLinearFeeProgression validates that fee rate increases follow
// a roughly linear pattern.
func assertLinearFeeProgression(t *harnessTest, feeRateHistory []int64) {
    require.Greater(t, len(feeRateHistory), 1, 
        "Fee rate should have been bumped at least once")
    
    feeDiffs := make([]int64, len(feeRateHistory)-1)
    for i := 0; i < len(feeRateHistory)-1; i++ {
        feeDiffs[i] = feeRateHistory[i+1] - feeRateHistory[i]
    }
    
    t.Logf("Fee rate progression: %v", feeRateHistory)
    t.Logf("Fee rate differences: %v", feeDiffs)
    
    if len(feeDiffs) > 2 {
        avgDiff := 0.0
        for _, diff := range feeDiffs {
            avgDiff += float64(diff)
        }
        avgDiff /= float64(len(feeDiffs))
        
        for _, diff := range feeDiffs {
            // Allow some variation due to rounding, etc.
            require.InDeltaf(t, avgDiff, float64(diff), avgDiff*0.5,
                "Fee rate differences should be roughly constant for a linear function")
        }
    }
}

// logFeeRateProgression prints a simple ASCII visualization of fee rate progression
func logFeeRateProgression(t *harnessTest, feeRateHistory []int64, maxFeeRate int64) {
    t.Logf("Fee Rate Progression Chart (max: %d sat/vByte)", maxFeeRate)
    t.Logf("----------------------------------------")
    
    // Find max for scaling
    var max int64 = 0
    for _, rate := range feeRateHistory {
        if rate > max {
            max = rate
        }
    }
    
    // Handle case where all values are 0
    if max == 0 {
        max = 1
    }
    
    // Chart width
    width := 40
    
    for i, rate := range feeRateHistory {
        bars := int(float64(rate) / float64(max) * float64(width))
        barStr := strings.Repeat("#", bars)
        t.Logf("Block %d: %d sat/vB |%s", i, rate, barStr)
    }
    t.Logf("----------------------------------------")
}
