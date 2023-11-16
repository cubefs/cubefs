package tpmonitor

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	percent90By1K  = 900
	percent99By1K  = 990
	percent999By1K = 999
	percent1KBase  = 1000

	defStepArrayLen = 326
)

const (
	CostStep5us   = 5                  //<100us  			20
	CostStep10us  = 10                 //100~1000us		90
	CostStep1ms   = 1000               //1~100ms			99
	CostStep100ms = 100 * CostStep1ms  //100~1000ms		9
	CostStep1s    = 1000 * CostStep1ms //1~100s			99
	CostStep100s  = 100 * CostStep1s   //100~max			9

	CostStep10usStart  = 100
	CostStep1msStart   = 1000
	CostStep100msStart = CostStep100ms
	CostStep1sStart    = CostStep1s
	CostStep100sStart  = CostStep100s
	CostStep100sEnd    = 10 * CostStep100s

	CostStep10usIndex  = 20
	CostStep1msIndex   = 110
	CostStep100msIndex = 209
	CostStep1sIndex    = 218
	CostStep100sIndex  = 317
)

var scale = make([]uint64, defStepArrayLen)
var stepArray = []uint64{CostStep5us, CostStep10us, CostStep1ms, CostStep100ms, CostStep1s, CostStep100s}
var stepStart = []uint64{CostStep10usStart, CostStep1msStart, CostStep100msStart, CostStep1sStart, CostStep100sStart, CostStep100sEnd}

func init() {
	sum := uint64(0)
	stepIndex := 0
	step := stepArray[stepIndex]
	for i := 0; i < defStepArrayLen; i++ {
		if sum >= stepStart[stepIndex] {
			stepIndex++
			step = stepArray[stepIndex]
		}
		scale[i] = sum
		sum += step
	}
}

//[0]=0	[1]=5	[2]=10	[3]=15	[4]=20	[5]=25	[6]=30	[7]=35	[8]=40	[9]=45
//[10]=50	[11]=55	[12]=60	[13]=65	[14]=70	[15]=75	[16]=80	[17]=85	[18]=90	[19]=95

//[20]=100	[21]=110	[22]=120	[23]=130	[24]=140	[25]=150	[26]=160	[27]=170	[28]=180	[29]=190
//[30]=200	[31]=210	[32]=220	[33]=230	[34]=240	[35]=250	[36]=260	[37]=270	[38]=280	[39]=290
//[40]=300	[41]=310	[42]=320	[43]=330	[44]=340	[45]=350	[46]=360	[47]=370	[48]=380	[49]=390
//[50]=400	[51]=410	[52]=420	[53]=430	[54]=440	[55]=450	[56]=460	[57]=470	[58]=480	[59]=490
//[60]=500	[61]=510	[62]=520	[63]=530	[64]=540	[65]=550	[66]=560	[67]=570	[68]=580	[69]=590
//[70]=600	[71]=610	[72]=620	[73]=630	[74]=640	[75]=650	[76]=660	[77]=670	[78]=680	[79]=690
//[80]=700	[81]=710	[82]=720	[83]=730	[84]=740	[85]=750	[86]=760	[87]=770	[88]=780	[89]=790
//[90]=800	[91]=810	[92]=820	[93]=830	[94]=840	[95]=850	[96]=860	[97]=870	[98]=880	[99]=890
//[100]=900	[101]=910	[102]=920	[103]=930	[104]=940	[105]=950	[106]=960	[107]=970	[108]=980	[109]=990

//[110]=1000	[111]=2000	[112]=3000	[113]=4000	[114]=5000	[115]=6000	[116]=7000	[117]=8000	[118]=9000	[119]=10000
//[120]=11000	[121]=12000	[122]=13000	[123]=14000	[124]=15000	[125]=16000	[126]=17000	[127]=18000	[128]=19000	[129]=20000
//[130]=21000	[131]=22000	[132]=23000	[133]=24000	[134]=25000	[135]=26000	[136]=27000	[137]=28000	[138]=29000	[139]=30000
//[140]=31000	[141]=32000	[142]=33000	[143]=34000	[144]=35000	[145]=36000	[146]=37000	[147]=38000	[148]=39000	[149]=40000
//[150]=41000	[151]=42000	[152]=43000	[153]=44000	[154]=45000	[155]=46000	[156]=47000	[157]=48000	[158]=49000	[159]=50000
//[160]=51000	[161]=52000	[162]=53000	[163]=54000	[164]=55000	[165]=56000	[166]=57000	[167]=58000	[168]=59000	[169]=60000
//[170]=61000	[171]=62000	[172]=63000	[173]=64000	[174]=65000	[175]=66000	[176]=67000	[177]=68000	[178]=69000	[179]=70000
//[180]=71000	[181]=72000	[182]=73000	[183]=74000	[184]=75000	[185]=76000	[186]=77000	[187]=78000	[188]=79000	[189]=80000
//[190]=81000	[191]=82000	[192]=83000	[193]=84000	[194]=85000	[195]=86000	[196]=87000	[197]=88000	[198]=89000	[199]=90000
//[200]=91000	[201]=92000	[202]=93000	[203]=94000	[204]=95000	[205]=96000	[206]=97000	[207]=98000	[208]=99000

//[209]=100000 [210]=200000	[211]=300000	[212]=400000	[213]=500000	[214]=600000	[215]=700000	[216]=800000	[217]=900000

//[218]=1000000   [219]=2000000   [220]=3000000 	[221]=4000000	[222]=5000000	[223]=6000000	[224]=7000000	[225]=8000000	[226]=9000000	[227]=10000000
//[228]=11000000  [229]=12000000  [230]=13000000	[231]=14000000	[232]=15000000	[233]=16000000	[234]=17000000	[235]=18000000	[236]=19000000	[237]=20000000
//[238]=21000000  [239]=22000000  [240]=23000000	[241]=24000000	[242]=25000000	[243]=26000000	[244]=27000000	[245]=28000000	[246]=29000000	[247]=30000000
//[248]=31000000  [249]=32000000  [250]=33000000	[251]=34000000	[252]=35000000	[253]=36000000	[254]=37000000	[255]=38000000	[256]=39000000	[257]=40000000
//[258]=41000000  [259]=42000000  [260]=43000000	[261]=44000000	[262]=45000000	[263]=46000000	[264]=47000000	[265]=48000000	[266]=49000000	[267]=50000000
//[268]=51000000  [269]=52000000  [270]=53000000	[271]=54000000	[272]=55000000	[273]=56000000	[274]=57000000	[275]=58000000	[276]=59000000	[277]=60000000
//[278]=61000000  [279]=62000000  [280]=63000000	[281]=64000000	[282]=65000000	[283]=66000000	[284]=67000000	[285]=68000000	[286]=69000000	[287]=70000000
//[288]=71000000  [289]=72000000  [290]=73000000	[291]=74000000	[292]=75000000	[293]=76000000	[294]=77000000	[295]=78000000	[296]=79000000	[297]=80000000
//[298]=81000000  [299]=82000000  [300]=83000000	[301]=84000000	[302]=85000000	[303]=86000000	[304]=87000000	[305]=88000000	[306]=89000000	[307]=90000000
//[308]=91000000  [309]=92000000  [310]=93000000	[311]=94000000	[312]=95000000	[313]=96000000	[314]=97000000	[315]=98000000	[316]=99000000

//[317]=100000000   [318]=200000000	[319]=300000000 [320]=400000000	[321]=500000000	[322]=600000000	[323]=700000000	[324]=800000000	[325]=900000000

func locateDelayIndex(delayus int) (index int) {
	if delayus >= CostStep100sEnd {
		return defStepArrayLen - 1
	} else if delayus >= CostStep100sStart {
		index = CostStep100sIndex + (delayus-CostStep100sStart)/CostStep100s
	} else if delayus >= CostStep1sStart {
		index = CostStep1sIndex + (delayus-CostStep1sStart)/CostStep1s
	} else if delayus >= CostStep100msStart {
		index = CostStep100msIndex + (delayus-CostStep100msStart)/CostStep100ms
	} else if delayus >= CostStep1msStart {
		index = CostStep1msIndex + (delayus-CostStep1msStart)/CostStep1ms
	} else if delayus >= CostStep10usStart {
		index = CostStep10usIndex + (delayus-CostStep10usStart)/CostStep10us
	} else {
		index = delayus / CostStep5us
	}
	return
}

type TpMonitor struct {
	sync.RWMutex
	max            uint64
	sum            uint64
	count          uint64
	countContainer []uint32
}

//NewTpMonitor
//unit: microsecond		us
func NewTpMonitor() (monitor *TpMonitor) {
	monitor = &TpMonitor{
		countContainer: make([]uint32, defStepArrayLen),
	}
	return monitor
}

func (monitor *TpMonitor) Accumulate(microCost int, start time.Time) {
	index := locateDelayIndex(microCost)
	monitor.RLock()
	defer monitor.RUnlock()
	atomic.AddUint64(&monitor.sum, uint64(microCost))
	atomic.AddUint64(&monitor.count, 1)
	atomic.AddUint32(&monitor.countContainer[index], 1)

	oldMax := atomic.LoadUint64(&monitor.max)
	for uint64(microCost) > oldMax {
		if atomic.CompareAndSwapUint64(&monitor.max, oldMax, uint64(microCost)) {
			break
		}
		oldMax = atomic.LoadUint64(&monitor.max)
	}
}

func (monitor *TpMonitor) reset() {
	for i := 0; i < defStepArrayLen; i++ {
		monitor.countContainer[i] = 0
	}
	monitor.count = 0
	monitor.sum = 0
	monitor.max = 0
}

func (monitor *TpMonitor) calcTp99() (tp99 uint64) {
	pos := uint32(((percent1KBase - percent99By1K) * monitor.count) / percent1KBase)
	scanned := uint32(0)

	for index := defStepArrayLen - 1; index >= 0; index-- {
		if 0 == monitor.countContainer[index] {
			continue
		}
		scanned += monitor.countContainer[index]
		if scanned > pos {
			tp99 = scale[index]
			break
		}
	}

	return
}

func (monitor *TpMonitor) CalcTp() (max, avg, tp99 uint64) {
	monitor.Lock()
	defer func() {
		monitor.reset()
		monitor.Unlock()
	}()

	if monitor.count == 0 {
		return
	}

	max = monitor.max
	avg = monitor.sum / monitor.count
	tp99 = monitor.calcTp99()
	return
}
