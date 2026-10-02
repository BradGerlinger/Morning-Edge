import server

def test_pct():
    assert round(server._pct(110,100),2)==10.0

def test_prelim_bias():
    m={"gap_pct":3,"pm_change_pct":1,"pm_volume":1_000_000,"avg_daily_volume":10_000_000,"dollar_volume":2_000_000_000}
    score,bias,p=server._score_prelim(m,2,1,10)
    assert score>0 and bias=="LONG"

def test_db():
    server.init_db()
