package tokocrypto

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/nafaz/spot-perp-scanner/internal/domain"
)

// DepthSnapshot is the REST snapshot required to seed a local MBX order book.
type DepthSnapshot struct {
	LastUpdateID int64
	Bids, Asks   [][]string
	Timestamp    time.Time
}

// DepthUpdate is one diff-depth event. Quantities are absolute; zero deletes a level.
type DepthUpdate struct {
	Symbol                    string
	FirstUpdateID, FinalUpdateID int64
	Bids, Asks                [][]string
	EventTime                 time.Time
}

type localBook struct {
	bids, asks map[float64]float64
	lastID     int64
	updatedAt  time.Time
	bridged    bool
}

// BookManager maintains synchronized Tokocrypto MBX spot books.
// A book is invisible until its REST snapshot has been bridged by a valid WS event.
type BookManager struct {
	mu       sync.RWMutex
	books    map[string]*localBook
	maxDepth int
	now      func() time.Time
}

func NewBookManager(maxDepth int) *BookManager {
	if maxDepth <= 0 { maxDepth = 100 }
	return &BookManager{books: make(map[string]*localBook), maxDepth: maxDepth, now: time.Now}
}

// Bootstrap installs a REST snapshot. Call after starting the WS subscription and buffering events.
func (m *BookManager) Bootstrap(symbol string, snapshot DepthSnapshot) error {
	if symbol == "" || snapshot.LastUpdateID <= 0 { return fmt.Errorf("invalid tokocrypto snapshot") }
	bids, err := levelsToMap(snapshot.Bids); if err != nil { return fmt.Errorf("snapshot bids: %w", err) }
	asks, err := levelsToMap(snapshot.Asks); if err != nil { return fmt.Errorf("snapshot asks: %w", err) }
	if len(bids)==0 || len(asks)==0 { return fmt.Errorf("snapshot must be two-sided") }
	at:=snapshot.Timestamp; if at.IsZero(){at=m.now()}
	m.mu.Lock(); m.books[symbol]=&localBook{bids:bids,asks:asks,lastID:snapshot.LastUpdateID,updatedAt:at}; m.mu.Unlock()
	return nil
}

// Apply applies a diff event. false means stale/duplicate; an error invalidates the book.
func (m *BookManager) Apply(update DepthUpdate) (bool,error) {
	if update.Symbol=="" || update.FirstUpdateID<=0 || update.FinalUpdateID<update.FirstUpdateID { return false,fmt.Errorf("invalid tokocrypto depth update") }
	bids,err:=levelsToMap(update.Bids); if err!=nil{return false,fmt.Errorf("update bids: %w",err)}
	asks,err:=levelsToMap(update.Asks); if err!=nil{return false,fmt.Errorf("update asks: %w",err)}
	m.mu.Lock(); defer m.mu.Unlock()
	book:=m.books[update.Symbol]
	if book==nil{return false,fmt.Errorf("tokocrypto book %s is not bootstrapped",update.Symbol)}
	if update.FinalUpdateID<=book.lastID{return false,nil}
	if !book.bridged {
		bridged := update.FirstUpdateID <= book.lastID+1 && update.FinalUpdateID >= book.lastID+1
		if update.PreviousUpdateID > 0 {
			bridged = update.PreviousUpdateID == book.lastID
		}
		if !bridged {
			delete(m.books,update.Symbol)
			return false,fmt.Errorf("tokocrypto snapshot bridge failed for %s at update %d (first=%d previous=%d final=%d)",update.Symbol,book.lastID+1,update.FirstUpdateID,update.PreviousUpdateID,update.FinalUpdateID)
		}
		book.bridged = true
	} else if update.PreviousUpdateID > 0 {
		if update.PreviousUpdateID != book.lastID {
			delete(m.books,update.Symbol)
			return false,fmt.Errorf("tokocrypto previous update gap for %s: expected previous %d, got %d",update.Symbol,book.lastID,update.PreviousUpdateID)
		}
	} else if update.FirstUpdateID != book.lastID+1 {
		delete(m.books,update.Symbol)
		return false,fmt.Errorf("tokocrypto sequence gap for %s: expected first update %d, got %d",update.Symbol,book.lastID+1,update.FirstUpdateID)
	}
	applyLevels(book.bids,bids); applyLevels(book.asks,asks)
	book.lastID=update.FinalUpdateID
	book.updatedAt=update.EventTime; if book.updatedAt.IsZero(){book.updatedAt=m.now()}
	return true,nil
}

// Get returns only a synchronized, fresh, two-sided local book.
func (m *BookManager) Get(symbol string,maxAge time.Duration)(domain.OrderBook,bool){
	m.mu.RLock(); book:=m.books[symbol]; if book==nil||!book.bridged{m.mu.RUnlock();return domain.OrderBook{},false}
	bids:=mapToLevels(book.bids,true,m.maxDepth); asks:=mapToLevels(book.asks,false,m.maxDepth)
	out:=domain.OrderBook{Exchange:"tokocrypto",Symbol:symbol,Bids:bids,Asks:asks,SequenceID:book.lastID,Timestamp:book.updatedAt,ReceivedAt:book.updatedAt,Source:"websocket"}
	m.mu.RUnlock()
	if len(bids)==0||len(asks)==0{return domain.OrderBook{},false}
	if maxAge>0 && time.Since(out.ReceivedAt)>maxAge{return domain.OrderBook{},false}
	return out,true
}

func (m *BookManager) Invalidate(symbol string){m.mu.Lock();delete(m.books,symbol);m.mu.Unlock()}
func levelsToMap(rows [][]string)(map[float64]float64,error){
	out:=make(map[float64]float64,len(rows))
	for _,row:=range rows{if len(row)<2{return nil,fmt.Errorf("level must contain price and quantity")}
		p,e1:=strconv.ParseFloat(row[0],64);q,e2:=strconv.ParseFloat(row[1],64)
		if e1!=nil||e2!=nil||p<=0||q<0||math.IsNaN(p)||math.IsInf(p,0)||math.IsNaN(q)||math.IsInf(q,0){return nil,fmt.Errorf("invalid price level")}
		out[p]=q
	}
	return out,nil
}
func applyLevels(dst,updates map[float64]float64){for p,q:=range updates{if q==0{delete(dst,p)}else{dst[p]=q}}}
func mapToLevels(src map[float64]float64,bids bool,limit int)[]domain.PriceLevel{
	prices:=make([]float64,0,len(src));for p,q:=range src{if q>0{prices=append(prices,p)}}
	sort.Float64s(prices);if bids{for i,j:=0,len(prices)-1;i<j;i,j=i+1,j-1{prices[i],prices[j]=prices[j],prices[i]}}
	if len(prices)>limit{prices=prices[:limit]}
	out:=make([]domain.PriceLevel,0,len(prices));for _,p:=range prices{out=append(out,domain.PriceLevel{Price:p,Quantity:src[p]})};return out
}
