package tokenmanager

import "testing"

func TestGetTokenSuccess(t *testing.T) {
    tokenM := NewTokenManager(2)
    if !tokenM.GetRunToken(1) {
        t.Fatalf("need success, but now failed\n")
    }

    if !tokenM.GetRunToken(2) {
        t.Fatalf("need success, but now failed\n")
    }

    tokenM.ReleaseToken(1)

    if !tokenM.GetRunToken(3) {
        t.Fatalf("need success, but now failed\n")
    }
}

func TestGetTokenFailed(t *testing.T) {
    tokenM := NewTokenManager(2)
    if !tokenM.GetRunToken(1) {
        t.Fatalf("need success, but now failed\n")
    }

    if !tokenM.GetRunToken(2) {
        t.Fatalf("need success, but now failed\n")
    }

    if tokenM.GetRunToken(3) {
        t.Fatalf("need success, but now failed\n")
    }
}

func TestResetTokenAdd(t *testing.T) {
    tokenM := NewTokenManager(2)
    if !tokenM.GetRunToken(1) {
        t.Fatalf("need success, but now failed\n")
    }

    if !tokenM.GetRunToken(2) {
        t.Fatalf("need success, but now failed\n")
    }

    if tokenM.GetRunningCnt() != 2 {
        t.Fatalf("running cnt, expect 2, but now:%d\n", tokenM.GetRunningCnt())
    }

    tokenM.ResetRunCnt(3)

    if tokenM.GetConfCnt() != 3 {
        t.Fatalf("conf cnt, expect 3, but now:%d\n", tokenM.GetConfCnt())
    }

    if !tokenM.GetRunToken(3) {
        t.Fatalf("need success, but now failed\n")
    }

    tokenM.ReleaseToken(3)

    if tokenM.GetRunToken(2) {
        //repeat
        t.Fatalf("need failed, but now success\n")
    }

    if !tokenM.GetRunToken(4) {
        t.Fatalf("need success, but now failed\n")
    }

    if tokenM.GetRunToken(5) {
        t.Fatalf("need failed, but now success\n")
    }
}

func TestResetTokenDec(t *testing.T) {
    tokenM := NewTokenManager(3)
    if !tokenM.GetRunToken(1) {
        t.Fatalf("need success, but now failed\n")
    }

    if !tokenM.GetRunToken(2) {
        t.Fatalf("need success, but now failed\n")
    }

    if !tokenM.GetRunToken(3) {
        t.Fatalf("need success, but now failed\n")
    }

    tokenM.ResetRunCnt(2)

    tokenM.ReleaseToken(3)

    if tokenM.GetRunToken(3) {
        //repeat
        t.Fatalf("need failed, but now success\n")
    }

    if tokenM.GetRunToken(4) {
        t.Fatalf("need success, but now failed\n")
    }

    tokenM.ReleaseToken(1)
    if tokenM.GetRunToken(4) {
        t.Fatalf("need failed, but now success\n")
    }

    if !tokenM.GetRunToken(3) {
        t.Fatalf("need success, but now failed\n")
    }

    tokenM.ReleaseToken(2)

    if !tokenM.GetRunToken(4) {
        t.Fatalf("need success, but now failed\n")
    }
}

func TestPutTokenFailed(t *testing.T) {
    tokenM := NewTokenManager(3)
    if !tokenM.GetRunToken(1) {
        t.Fatalf("need success, but now failed\n")
    }

    if !tokenM.GetRunToken(2) {
        t.Fatalf("need success, but now failed\n")
    }

    if !tokenM.GetRunToken(3) {
        t.Fatalf("need success, but now failed\n")
    }

    tokenM.ReleaseToken(4)

    if tokenM.GetRunToken(4) {
        t.Fatalf("need failed, but now success\n")
    }

    tokenM.ReleaseToken(2)

    if !tokenM.GetRunToken(4) {
        t.Fatalf("need success, but now failed\n")
    }
}