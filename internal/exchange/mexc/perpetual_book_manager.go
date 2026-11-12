package mexc

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

// PerpetualDepthSnapshot is the REST depth bootstrap for one contract.
type PerpetualDepthSnapshot struct {
	Version int64
	Bids, Asks [][]float64
	Timestamp time.Time
}

// PerpetualDepthUpdate is an incremental push.depth update. Begin/End are used
// by current MEXC payloads; Version is accepted for legacy single-version frames.
type PerpetualDepthUpdate struct {
	Symbol string
	Begin, End int64
	Version int64
	Bids, Asks [][]float64
	Timestamp time.Time
	ContractSize float64
}

type contractBook struct {
	bids,asks map[float64]float64
	version int64
	contractSize float64
	updatedAt time.Time
	synced bool
}

// PerpetualBookManager maintains version-synchronized MEXC contract books.
type PerpetualBookManager struct {
	mu sync.RWMutex
	books map[string]*contractBook
	maxDepth int
	now func()time.Time
}

func NewPerpetualBookManager(maxDepth int)*PerpetualBookManager{
	if maxDepth<=0{maxDepth=100}
	return &PerpetualBookManager{books:make(map[string]*contractBook),maxDepth:maxDepth,now:time.Now}
}

func (m *PerpetualBookManager) Bootstrap(symbol string,snapshot PerpetualDepthSnapshot,contractSize float64)error{
	if symbol==""||snapshot.Version<=0||!validPositive(contractSize){return fmt.Errorf("invalid mexc perpetual snapshot")}
	bids,err:=contractLevelsToMap(snapshot.Bids,contractSize);if err!=nil{return fmt.Errorf("snapshot bids: %w",err)}
	asks,err:=contractLevelsToMap(snapshot.Asks,contractSize);if err!=nil{return fmt.Errorf("snapshot asks: %w",err)}
	if len(bids)==0||len(asks)==0{return fmt.Errorf("snapshot must be two-sided")}
	at:=snapshot.Timestamp;if at.IsZero(){at=m.now()}
	m.mu.Lock();m.books[symbol]=&contractBook{bids:bids,asks:asks,version:snapshot.Version,contractSize:contractSize,updatedAt:at};m.mu.Unlock()
	return nil
}

// Apply accepts only the next exact version. A gap invalidates the book so callers
// must fetch a new snapshot before publishing again.
func (m *PerpetualBookManager) Apply(update PerpetualDepthUpdate)(bool,error){
	if update.Symbol==""{return false,fmt.Errorf("missing mexc contract symbol")}
	begin,end:=update.Begin,update.End
	if begin==0&&end==0{begin,end=update.Version,update.Version}
	if begin<=0||end<begin{return false,fmt.Errorf("invalid mexc depth version range")}
	m.mu.Lock();defer m.mu.Unlock()
	book:=m.books[update.Symbol];if book==nil{return false,fmt.Errorf("mexc book %s is not bootstrapped",update.Symbol)}
	if end<=book.version{return false,nil}
	if begin!=book.version+1{delete(m.books,update.Symbol);return false,fmt.Errorf("mexc sequence gap for %s: expected %d, got %d",update.Symbol,book.version+1,begin)}
	if update.ContractSize>0&&math.Abs(update.ContractSize-book.contractSize)>1e-12{delete(m.books,update.Symbol);return false,fmt.Errorf("mexc contract size changed for %s",update.Symbol)}
	bids,err:=contractLevelsToMap(update.Bids,book.contractSize);if err!=nil{delete(m.books,update.Symbol);return false,fmt.Errorf("update bids: %w",err)}
	asks,err:=contractLevelsToMap(update.Asks,book.contractSize);if err!=nil{delete(m.books,update.Symbol);return false,fmt.Errorf("update asks: %w",err)}
	applyContractLevels(book.bids,bids);applyContractLevels(book.asks,asks)
	book.version=end;book.synced=true;book.updatedAt=update.Timestamp;if book.updatedAt.IsZero(){book.updatedAt=m.now()}
	return true,nil
}

func (m *PerpetualBookManager)Get(symbol string,maxAge time.Duration)(domain.OrderBook,bool){
	m.mu.RLock();book:=m.books[symbol];if book==nil||!book.synced{m.mu.RUnlock();return domain.OrderBook{},false}
	bids:=contractMapLevels(book.bids,true,m.maxDepth);asks:=contractMapLevels(book.asks,false,m.maxDepth)
	out:=domain.OrderBook{Exchange:"mexc",Symbol:symbol,Bids:bids,Asks:asks,SequenceID:book.version,Timestamp:book.updatedAt,ReceivedAt:book.updatedAt,Source:"websocket"}
	m.mu.RUnlock()
	if len(bids)==0||len(asks)==0{return domain.OrderBook{},false}
	if maxAge>0&&time.Since(out.ReceivedAt)>maxAge{return domain.OrderBook{},false}
	return out,true
}
func(m *PerpetualBookManager)Invalidate(symbol string){m.mu.Lock();delete(m.books,symbol);m.mu.Unlock()}
func contractLevelsToMap(rows [][]float64,size float64)(map[float64]float64,error){
	out:=make(map[float64]float64,len(rows))
	for _,row:=range rows{if len(row)<2{return nil,fmt.Errorf("level must contain price and volume")};p,q:=row[0],row[1]
		if !validPositive(p)||!validNonNegative(q){return nil,fmt.Errorf("invalid contract level")}
		out[p]=q*size
	}
	return out,nil
}
func applyContractLevels(dst,updates map[float64]float64){for p,q:=range updates{if q==0{delete(dst,p)}else{dst[p]=q}}}
func contractMapLevels(src map[float64]float64,bids bool,limit int)[]domain.PriceLevel{
	prices:=make([]float64,0,len(src));for p,q:=range src{if q>0{prices=append(prices,p)}}
	sort.Float64s(prices);if bids{for i,j:=0,len(prices)-1;i<j;i,j=i+1,j-1{prices[i],prices[j]=prices[j],prices[i]}}
	if len(prices)>limit{prices=prices[:limit]}
	out:=make([]domain.PriceLevel,0,len(prices));for _,p:=range prices{out=append(out,domain.PriceLevel{Price:p,Quantity:src[p]})};return out
}
