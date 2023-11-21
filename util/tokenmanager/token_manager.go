package tokenmanager

import (
    "container/list"
    "sync"
)

const (
    defMaxWaitToken  = 10 * 10000            //10W
)

type TokenManager struct {
    mu            sync.RWMutex
    runningId     []uint64
    waitIdList    *list.List
    runningCount  uint64
}

func NewTokenManager(runCnt uint64) *TokenManager {
    tokenM := &TokenManager{}
    tokenM.Init(runCnt)
    return tokenM
}

func (tokeM *TokenManager) Init(runCnt uint64) {
    tokeM.runningId = make([]uint64, runCnt)
    tokeM.waitIdList = list.New()
}

func (tokeM *TokenManager) ResetRunCnt(newRunCnt uint64) {
    tokeM.mu.Lock()
    defer tokeM.mu.Unlock()
    newRunningId :=  make([]uint64, newRunCnt)

    runningCnt := uint64(0)
    for _, key := range tokeM.runningId {
        if runningCnt >= newRunCnt {
            break
        }
        if key != 0 {
            newRunningId[int(runningCnt)] = key
            runningCnt++
        }
    }

    tokeM.runningCount = runningCnt
    tokeM.runningId    = newRunningId
}

func (tokeM *TokenManager) GetConfCnt()(confCnt uint64) {
    tokeM.mu.Lock()
    defer tokeM.mu.Unlock()
    confCnt = uint64(len(tokeM.runningId))
    return
}

func (tokeM *TokenManager) GetRunningCnt()(Cnt uint64) {
    tokeM.mu.Lock()
    defer tokeM.mu.Unlock()
    Cnt = tokeM.runningCount
    return
}

func (tokeM *TokenManager) GetRunningIds()(Cnt uint64, RunningIds []uint64) {
    tokeM.mu.Lock()
    defer tokeM.mu.Unlock()
    RunningIds = make([]uint64, 0)
    Cnt = 0
    for _, key := range tokeM.runningId {
        if key != 0 {
            RunningIds = append(RunningIds, key)
            Cnt++
        }
    }
    return
}

func (tokeM *TokenManager) GetRunToken(Key uint64) (canExe bool){
    tokeM.mu.Lock()
    defer tokeM.mu.Unlock()

    defer func() {
        for ; tokeM.waitIdList.Len() > defMaxWaitToken; {
            tokeM.waitIdList.Remove(tokeM.waitIdList.Front())
        }

        if canExe {
            for index := 0; index < len(tokeM.runningId); index++ {
                if tokeM.runningId[index] == 0 {
                    tokeM.runningId[index] = Key
                    tokeM.runningCount++
                    return
                }
            }
        }
    }()

    canExe = false
    for i := 0; i < len(tokeM.runningId); i++ {
        if tokeM.runningId[i] == Key {
            //running ,do nothing
            return
        }
    }

    found := false
    for iter := tokeM.waitIdList.Front(); iter != nil; iter = iter.Next() {
        if iter.Value.(uint64) == Key {
            found = true
            break
        }
    }

    if !found {
        tokeM.waitIdList.PushBack(Key)
    }
    runningCnt := int(tokeM.runningCount)
    for iter := tokeM.waitIdList.Front() ; runningCnt < len(tokeM.runningId) && iter != nil; iter = iter.Next() {
        if iter.Value.(uint64) == Key {
            tokeM.waitIdList.Remove(iter)
            canExe = true
            return
        }
        runningCnt++
    }
    return
}

func (tokeM *TokenManager) ReleaseToken(Key uint64) {
    tokeM.mu.Lock()
    defer tokeM.mu.Unlock()
    for i := 0; i < len(tokeM.runningId); i++ {
        if tokeM.runningId[i] == Key {
            tokeM.runningId[i] = 0
            tokeM.runningCount--
            break
        }
    }

    for iter := tokeM.waitIdList.Front(); iter != nil; iter = iter.Next() {
        if iter.Value.(uint64) == Key {
            tokeM.waitIdList.Remove(iter)
            break
        }
    }
    return
}
