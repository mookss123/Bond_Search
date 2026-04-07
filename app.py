"""
Börse Frankfurt Bond Search
"""
import re
import streamlit as st
import requests
import pandas as pd
import hashlib
from datetime import datetime, timedelta, timezone
from io import BytesIO
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Constants ─────────────────────────────────────────────────────────────────
SALT       = "af5a8d16eb5dc49f8a72b26fd9185475c7a"
BASE_URL   = "https://api.live.deutsche-boerse.com"
TZ_OFFSET  = timezone(timedelta(hours=8))
MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun",
               "Jul","Aug","Sep","Oct","Nov","Dec"]
DISPLAY_COLS = [
    "ISIN", "名稱", "Last Price", "Coupon (%)", "Yield (%)",
    "Maturity Date", "First Interest Pay Date",
    "Interest Payment Cycle", "Coupon Months",
    "Issue Currency", "Min. Investment Amount", "Min. Tradeable Unit",
    "Subordinated", "Trading Model",
]

# ── API ───────────────────────────────────────────────────────────────────────
def _make_headers(full_url):
    now         = datetime.now(tz=TZ_OFFSET)
    client_date = now.strftime("%Y-%m-%dT%H:%M:%S+08:00")
    x_security  = hashlib.md5(now.strftime("%Y%m%d%H%M").encode()).hexdigest()
    x_traceid   = hashlib.md5((client_date + full_url + SALT).encode()).hexdigest()
    return {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://live.deutsche-boerse.com",
        "Referer": "https://live.deutsche-boerse.com/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "client-date": client_date,
        "x-security": x_security,
        "x-client-traceid": x_traceid,
        "content-type": "application/json",
    }

def _get(path, params, timeout=25):
    qs       = urlencode(params)
    full_url = f"{BASE_URL}{path}?{qs}" if qs else f"{BASE_URL}{path}"
    for attempt in range(2):
        try:
            r = requests.get(full_url, headers=_make_headers(full_url), timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.Timeout:
            if attempt == 1: raise
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 500:
                return {}
            raise
    return {}

def search_bonds(query, page_size=50):
    # API 單次最多 50，超過要分頁
    all_hits = []
    pages = (page_size + 49) // 50  # 每頁 50 筆
    for page in range(1, pages + 1):
        raw = _get("/v1/global_search/pagedsearch/bond/en",
                   {"searchTerms": query, "page": page, "pageSize": 50})
        hits = raw.get("result") or raw.get("data") or []
        all_hits.extend(hits)
        total = raw.get("total") or 0
        if len(all_hits) >= total or len(all_hits) >= page_size or not hits:
            break
    return {"result": all_hits[:page_size], "total": len(all_hits)}

def get_master_data(isin):
    return _get("/v1/data/master_data_bond", {"isin": isin})

def get_trading_params(isin):
    return _get("/v1/data/frankfurt_trading_parameter", {"isin": isin})

def get_interest_widget(isin):
    return _get("/v1/data/interest_rate_widget", {"isin": isin})

def get_price_history(isin, start, end, mic="XFRA"):
    for attempt in range(2):
        try:
            return _get("/v1/data/price_history",
                        {"isin": isin, "mic": mic, "minDate": start, "maxDate": end,
                         "limit": 500, "offset": 0}, timeout=45)
        except requests.exceptions.Timeout:
            if attempt == 1: raise
    return {}

def get_last_price(isin):
    """最近 10 天取最新 Close"""
    today = datetime.now(tz=TZ_OFFSET)
    end   = today.strftime("%Y-%m-%d")
    start = (today - timedelta(days=10)).strftime("%Y-%m-%d")
    data  = _get("/v1/data/price_history",
                 {"isin": isin, "mic": "XFRA", "minDate": start, "maxDate": end,
                  "limit": 5, "offset": 0}, timeout=20)
    rows  = data.get("data") or []
    if not rows: return "—"
    return sorted(rows, key=lambda r: r.get("date",""), reverse=True)[0].get("close","—")

# ── Helpers ───────────────────────────────────────────────────────────────────
def _safe(d, *keys, default="—"):
    for k in keys:
        if not isinstance(d, dict): return default
        d = d.get(k)
        if d is None: return default
    return d if d not in (None,"",[],{}) else default

def _tr(field, default="—"):
    if isinstance(field, dict):
        return (field.get("translations",{}).get("en")
                or field.get("originalValue") or default)
    return str(field) if field not in (None,"") else default

def _name(bond):
    for key in ("name","instrumentName"):
        f = bond.get(key)
        if isinstance(f, dict):
            return f.get("translations",{}).get("en") or f.get("originalValue") or "—"
        if isinstance(f, str) and f: return f
    return "—"

def parse_maturity_year(query):
    m = re.search(r'\b\d{2}/(\d{2})\b', query)
    return (2000 + int(m.group(1))) if m else None

def calc_coupon_months(first_pay, cycle_en, cycle_orig):
    if not first_pay or first_pay == "—": return "—"
    try: month = int(str(first_pay)[5:7])
    except: return "—"
    cv = str(cycle_en).lower()
    if "monthly" in cv:     interval = 1
    elif "quarterly" in cv: interval = 3
    elif "semi" in cv:      interval = 6
    elif "annual" in cv:    interval = 12
    else:
        interval = {"1":12,"2":6,"3":3,"4":1}.get(str(cycle_orig).strip(), 0)
        if not interval: return f"({cycle_en or cycle_orig})"
    months = sorted({((month-1+i*interval)%12)+1 for i in range(12//interval)})
    return ", ".join(MONTH_NAMES[m-1] for m in months)

def fetch_bond_data(bond):
    """並行抓一個 ISIN 的所有基本資料"""
    isin = _safe(bond, "isin")
    res  = {}
    def call(name, fn):
        try:    res[name] = fn(isin)
        except Exception as e: res[name] = {"_error": str(e)}
    with ThreadPoolExecutor(max_workers=4) as ex:
        fs = [ex.submit(call, "master",   get_master_data),
              ex.submit(call, "trading",  get_trading_params),
              ex.submit(call, "interest", get_interest_widget),
              ex.submit(call, "price",    get_last_price)]
        for f in as_completed(fs): pass
    return isin, bond, res

def build_row(isin, bond, master, trading, interest, last_price):
    first_pay  = _safe(interest,"firstAnnualPayDate") or _safe(master,"firstAnnualPayDate")
    cycle_raw  = _safe(interest,"interestPaymentCycle",default={})
    cycle_en   = cycle_raw.get("translations",{}).get("en","") if isinstance(cycle_raw,dict) else ""
    cycle_orig = cycle_raw.get("originalValue","") if isinstance(cycle_raw,dict) else str(cycle_raw)
    lp = last_price if not isinstance(last_price, dict) else "—"
    coupon = _safe(master,"cupon")
    try:    yld = round(float(coupon)/float(lp)*100, 4) if lp!="—" and coupon!="—" else "—"
    except: yld = "—"
    return {
        "ISIN":                    isin,
        "名稱":                    _name(bond),
        "Last Price":              lp,
        "Coupon (%)":              coupon,
        "Yield (%)":               yld,
        "Maturity Date":           _safe(master,"maturity"),
        "First Interest Pay Date": first_pay,
        "Interest Payment Cycle":  cycle_en or cycle_orig or "—",
        "Coupon Months":           calc_coupon_months(first_pay, cycle_en, cycle_orig),
        "Issue Currency":          _safe(master,"issueCurrency"),
        "Min. Investment Amount":  _safe(master,"minimumInvestmentAmount"),
        "Min. Tradeable Unit":     _safe(trading,"minimumTradableUnit"),
        "Subordinated":            _safe(master,"subordinated"),
        "Trading Model":           _tr(_safe(trading,"tradingModel",default={})),
    }

def build_history_df(hist):
    rows = hist.get("data") or []
    if not rows: return pd.DataFrame()
    df = pd.DataFrame(rows)
    df.rename(columns={"date":"Date","open":"Open","close":"Close",
                        "high":"High","low":"Low",
                        "turnoverPieces":"Volume","turnoverEuro":"Turnover(EUR)"},
              inplace=True)
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"])
        df.sort_values("Date", ascending=False, inplace=True)
        df.reset_index(drop=True, inplace=True)
    return df

def to_excel(summary_df, history_dict):
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        summary_df.to_excel(w, sheet_name="Summary", index=False)
        # 所有 Price History 合併成一個 Sheet，加 ISIN 欄位區分
        all_hist = []
        for key, hdf in history_dict.items():
            if hdf is not None and not hdf.empty:
                tmp = hdf.copy()
                # key 格式: "ISIN|start|end" 或 "ISIN"
                parts = key.split("|")
                tmp.insert(0, "ISIN", parts[0])
                if len(parts) >= 3:
                    tmp.insert(1, "Period", f"{parts[1]}~{parts[2]}" if parts[1]!=parts[2] else parts[1])
                all_hist.append(tmp)
        if all_hist:
            pd.concat(all_hist, ignore_index=True).to_excel(
                w, sheet_name="Price History", index=False)
    return buf.getvalue()

def parse_dates_input(text):
    """
    解析使用者輸入的日期，支援：
    - 單日：2023-10-20
    - 多日：2023-10-20, 2023-11-01
    - 區間：2023-10-20 ~ 2023-11-01
    回傳 list of (start, end) tuples
    """
    text = text.strip()
    if not text: return []
    results = []
    for part in text.replace("，",",").split(","):
        part = part.strip()
        if "~" in part:
            s, e = [x.strip() for x in part.split("~",1)]
            try:
                datetime.strptime(s,"%Y-%m-%d")
                datetime.strptime(e,"%Y-%m-%d")
                results.append((s, e))
            except: pass
        else:
            try:
                datetime.strptime(part,"%Y-%m-%d")
                results.append((part, part))
            except: pass
    return results

# ── UI ────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Börse Frankfurt Bond Search",
                   page_icon="🏦", layout="wide")
st.title("🏦 Börse Frankfurt — Bond Search")
st.caption("資料來源：live.deutsche-boerse.com（延遲 ~15 分鐘）")

# Session state
for k, v in [("rows",[]), ("cart",[]), ("ph_cache",{}), ("ph_selected",[])]:
    if k not in st.session_state:
        st.session_state[k] = v

# ── 搜尋列 ───────────────────────────────────────────────────────────────────
c1, c2, c3 = st.columns([4, 1, 1])
with c1:
    query = st.text_input("搜尋",
        placeholder="例如：25/55  |  EDF  |  Microsoft  |  ISIN",
        label_visibility="collapsed")
with c2:
    page_size = st.selectbox("最多筆數", [20,50,100,200,500,1000], index=1,
                              label_visibility="collapsed")
with c3:
    do_search = st.button("🔍 搜尋", type="primary", use_container_width=True)

# ── 搜尋執行 ──────────────────────────────────────────────────────────────────
if do_search and query.strip():
    st.session_state.ph_cache    = {}
    st.session_state.ph_selected = []
    with st.spinner(f"搜尋「{query}」..."):
        try:
            raw  = search_bonds(query.strip(), page_size=page_size)
            hits = raw.get("result") or raw.get("data") or []

            if not hits:
                st.warning("找不到結果")
                st.session_state.rows = []
            else:
                prog  = st.progress(0, text="載入資料...")
                rows  = []
                order = [_safe(b,"isin") for b in hits]
                done  = [0]
                with ThreadPoolExecutor(max_workers=5) as ex:
                    for f in as_completed({ex.submit(fetch_bond_data,b):b for b in hits}):
                        isin, bond, d = f.result()
                        lp = d.get("price","—")
                        if isinstance(lp, dict): lp = "—"
                        rows.append(build_row(
                            isin, bond,
                            d.get("master",{}), d.get("trading",{}),
                            d.get("interest",{}), lp))
                        done[0] += 1
                        prog.progress(done[0]/len(hits),
                                      text=f"載入中 {done[0]}/{len(hits)}")
                prog.empty()
                # 到期年份過濾（用實際 Maturity Date）
                target_year = parse_maturity_year(query.strip())
                if target_year:
                    before = len(rows)
                    def maturity_ok(r):
                        mat = str(r.get("Maturity Date",""))
                        if not mat or mat == "—": return True  # 沒資料的保留
                        return mat.startswith(str(target_year))
                    rows = [r for r in rows if maturity_ok(r)]
                    removed = before - len(rows)
                    if removed > 0:
                        st.session_state["maturity_msg"] = f"ℹ️ 已移除非 {target_year} 年到期結果（移除 {removed} 筆）"
                    else:
                        st.session_state["maturity_msg"] = ""
                else:
                    st.session_state["maturity_msg"] = ""
                rows.sort(key=lambda r: order.index(r["ISIN"]) if r["ISIN"] in order else 999)
                st.session_state.rows = rows
                st.rerun()
        except Exception as e:
            st.error(f"❌ 搜尋失敗：{e}")

# ── 結果顯示 ──────────────────────────────────────────────────────────────────
rows = st.session_state.rows
if rows:
    df_all = pd.DataFrame(rows)
    if st.session_state.get("maturity_msg"):
        st.caption(st.session_state["maturity_msg"])
    st.success(f"共載入 **{len(rows)}** 筆")

    # ── 篩選 ─────────────────────────────────────────────────────────────
    with st.expander("🔽 篩選條件", expanded=True):
        f1, f2, f3 = st.columns(3)
        with f1:
            all_cur = sorted(df_all["Issue Currency"].dropna().unique().tolist())
            sel_cur = st.multiselect("Issue Currency", all_cur, default=all_cur, key="f_cur")
        with f2:
            all_mo = []
            for cm in df_all["Coupon Months"].dropna():
                for m in str(cm).split(", "):
                    if m.strip() and m.strip()!="—" and m.strip() not in all_mo:
                        all_mo.append(m.strip())
            all_mo = sorted(all_mo, key=lambda x: MONTH_NAMES.index(x) if x in MONTH_NAMES else 99)
            sel_mo = st.multiselect("配息月份", all_mo, default=[], key="f_mo")
        with f3:
            v_c = pd.to_numeric(df_all["Coupon (%)"], errors="coerce").dropna()
            if not v_c.empty and v_c.min()<v_c.max():
                c_range = st.slider("Coupon (%) 範圍",
                    float(v_c.min()), float(v_c.max()),
                    (float(v_c.min()), float(v_c.max())),
                    step=0.1, format="%.1f%%", key="f_c")
            else:
                c_range = (0.0, 99.0)
        f4, f5 = st.columns(2)
        with f4:
            sel_sub = st.radio("Subordinated",
                ["全部","❌ 否","✅ 是"], index=0, horizontal=True, key="f_sub")
        with f5:
            v_mia = pd.to_numeric(df_all["Min. Investment Amount"], errors="coerce").dropna()
            mia_limit = st.number_input("Min. Investment Amount 上限",
                min_value=0.0,
                max_value=float(v_mia.max()) if not v_mia.empty else 1e9,
                value=float(v_mia.max()) if not v_mia.empty else 1e9,
                step=1000.0, format="%.0f", key="f_mia") if not v_mia.empty else 1e9

    # 套用篩選
    df = df_all.copy()
    if sel_cur: df = df[df["Issue Currency"].isin(sel_cur)]
    if sel_mo:
        df = df[df["Coupon Months"].apply(
            lambda cm: any(m in str(cm).split(", ") for m in sel_mo)
            if cm and cm!="—" else False)]
    df["_c"] = pd.to_numeric(df["Coupon (%)"], errors="coerce")
    df = df[df["_c"].between(c_range[0], c_range[1]) | df["_c"].isna()]
    if sel_sub=="❌ 否":
        df = df[df["Subordinated"].apply(
            lambda x: x is False or str(x).lower() in ("false","none","—","null"))]
    elif sel_sub=="✅ 是":
        df = df[df["Subordinated"].apply(lambda x: x is True or str(x).lower()=="true")]
    df["_m"] = pd.to_numeric(df["Min. Investment Amount"], errors="coerce")
    df = df[df["_m"].isna() | (df["_m"] <= mia_limit)]

    # 排序
    def sort_key(r):
        cm = str(r.get("Coupon Months",""))
        fm = cm.split(", ")[0] if cm and cm!="—" else "zzz"
        return (str(r.get("Issue Currency","zzz")),
                MONTH_NAMES.index(fm) if fm in MONTH_NAMES else 99,
                str(r.get("Maturity Date","9999")))
    df_sorted = pd.DataFrame(sorted(df.to_dict("records"), key=sort_key))

    # toggle + 計數
    cart_isins = {r["ISIN"] for r in st.session_state.cart}
    col_a, col_b = st.columns([3,1])
    with col_a:
        st.caption(f"篩選後：**{len(df_sorted)}** 筆　|　購物車：**{len(cart_isins)}** 筆")
    with col_b:
        show_cart_only = st.toggle("只顯示打勾項目", value=False, key="cart_only")
    if show_cart_only and cart_isins:
        df_sorted = df_sorted[df_sorted["ISIN"].isin(cart_isins)]

    # ── 主表格 ───────────────────────────────────────────────────────────
    show_cols     = [c for c in DISPLAY_COLS if c in df_sorted.columns]
    no_isin_cols  = [c for c in show_cols if c != "ISIN"]
    widths        = [0.5, 1.5] + [2]*len(no_isin_cols)

    hdr = st.columns(widths)
    hdr[0].markdown("**🛒**")
    hdr[1].markdown("**ISIN**")
    for i, c in enumerate(no_isin_cols): hdr[i+2].markdown(f"**{c}**")
    st.markdown("---")

    seen_isins = set()
    for row_idx, row in enumerate(df_sorted.to_dict("records")):
        isin = row["ISIN"]
        if isin in seen_isins:
            continue  # 跳過重複
        seen_isins.add(isin)

        cb_key = f"bond_cart_cb_{isin}"
        if cb_key not in st.session_state:
            st.session_state[cb_key] = isin in cart_isins

        cols = st.columns(widths)
        checked = cols[0].checkbox("", key=cb_key, label_visibility="collapsed")

        if checked and isin not in cart_isins:
            st.session_state.cart.append(row); cart_isins.add(isin)
        elif not checked and isin in cart_isins:
            st.session_state.cart = [r for r in st.session_state.cart if r["ISIN"]!=isin]
            cart_isins.discard(isin)

        cols[1].write(isin)
        for i, c in enumerate(no_isin_cols):
            val = row.get(c,"—")
            if val is None or (isinstance(val,float) and pd.isna(val)): val="—"
            if isinstance(val, float):
                if c in ("Last Price",):          val = f"{val:.4f}"
                elif c in ("Coupon (%)","Yield (%)"): val = f"{val:.3f}%"
                elif c == "Min. Investment Amount":   val = f"{val:,.0f}"
                else:                                 val = str(val)
            cols[i+2].write(str(val))

    # ── Price History ─────────────────────────────────────────────────────
    st.divider()
    st.subheader("📈 Price History")

    names     = {r["ISIN"]: r["名稱"] for r in rows}
    isin_list = [r["ISIN"] for r in rows]

    # 購物車自動帶入
    cart_list = [r["ISIN"] for r in st.session_state.cart if r["ISIN"] in isin_list]
    prev_sel  = [i for i in st.session_state.ph_selected if i in isin_list]
    if set(cart_list) - set(prev_sel):
        merged = list(dict.fromkeys(cart_list + prev_sel))
        st.session_state.ph_selected = merged
        st.session_state["ph_ms"] = merged

    ph_selected = st.multiselect(
        "選擇要查看的債券",
        options=isin_list,
        default=[i for i in st.session_state.ph_selected if i in isin_list],
        format_func=lambda x: f"{x} — {names.get(x,'')}",
        key="ph_ms",
    )
    st.session_state.ph_selected = ph_selected

    # 日期輸入
    st.caption("輸入日期（支援：單日 `2023-10-20`、多日 `2023-10-20, 2023-11-01`、區間 `2023-10-20 ~ 2023-11-01`）")
    date_input_str = st.text_input("日期", placeholder="2023-10-20, 2023-11-01  或  2023-10-01 ~ 2023-12-31",
                                   label_visibility="collapsed", key="ph_dates")
    date_ranges = parse_dates_input(date_input_str)

    if ph_selected and date_ranges:
        if st.button("📥 載入 Price History", type="primary"):
            prog2 = st.progress(0, text="載入中...")
            tasks = [(isin, s, e) for isin in ph_selected for s, e in date_ranges]
            done2 = [0]

            def load_hist(isin, start, end):
                try:
                    hist = get_price_history(isin, start, end)
                    return isin, start, end, build_history_df(hist), None
                except Exception as ex:
                    return isin, start, end, pd.DataFrame(), str(ex)

            with ThreadPoolExecutor(max_workers=4) as ex:
                futs = {ex.submit(load_hist, *t): t for t in tasks}
                for f in as_completed(futs):
                    isin, start, end, hdf, err = f.result()
                    key = f"{isin}|{start}|{end}"
                    st.session_state.ph_cache[key] = hdf
                    done2[0] += 1
                    prog2.progress(done2[0]/len(tasks),
                                   text=f"載入中 {done2[0]}/{len(tasks)}")
                    if err and "timeout" in err.lower():
                        st.warning(f"{isin} 逾時")
            prog2.empty()

    # 顯示結果
    for isin in ph_selected:
        name = names.get(isin, isin)
        isin_keys = [(k, *k.split("|")[1:]) for k in st.session_state.ph_cache
                     if k.startswith(f"{isin}|")]
        if not isin_keys: continue

        with st.expander(f"**{name}** `{isin}`", expanded=True):
            for key, start, end in sorted(isin_keys, key=lambda x: x[1]):
                hdf = st.session_state.ph_cache[key]
                is_single = start == end
                label = start if is_single else f"{start} ～ {end}"

                if hdf.empty:
                    st.info(f"{label}：無資料")
                    continue

                if is_single:
                    # 單日：metric 顯示
                    close_val = hdf.iloc[0]["Close"] if "Close" in hdf.columns else "—"
                    actual_date = hdf.iloc[0]["Date"].strftime("%Y-%m-%d") if "Date" in hdf.columns else start
                    c1, c2 = st.columns(2)
                    c1.metric("📅 日期", actual_date)
                    c2.metric("💰 Close Price", f"{close_val:.4f}" if isinstance(close_val, float) else close_val)
                else:
                    # 區間：折線圖 + 表格
                    st.markdown(f"**{label}**　共 {len(hdf)} 筆")
                    if "Close" in hdf.columns and "Date" in hdf.columns:
                        st.line_chart(hdf.sort_values("Date").set_index("Date")[["Close"]], height=200)
                    st.dataframe(hdf, use_container_width=True, hide_index=True)

# ── 購物車 ────────────────────────────────────────────────────────────────────
st.divider()
st.subheader("🛒 購物車")
cart = st.session_state.cart
if not cart:
    st.caption("尚未加入任何債券，在上方表格打勾即可加入。")
else:
    st.success(f"已選 **{len(cart)}** 筆")
    cart_df = pd.DataFrame(cart)
    show_c  = [c for c in DISPLAY_COLS if c in cart_df.columns]
    st.dataframe(cart_df[show_c], use_container_width=True, hide_index=True,
                 column_config={
                     "Last Price": st.column_config.NumberColumn(format="%.4f"),
                     "Coupon (%)": st.column_config.NumberColumn(format="%.3f%%"),
                     "Yield (%)":  st.column_config.NumberColumn(format="%.4f%%"),
                 })
    ts = datetime.today().strftime("%Y%m%d_%H%M")
    ca, cb, cc, _ = st.columns([2,2,2,2])
    with ca:
        st.download_button("⬇️ CSV",
            data=cart_df[show_c].to_csv(index=False).encode("utf-8-sig"),
            file_name=f"cart_{ts}.csv", mime="text/csv",
            type="primary", use_container_width=True)
    with cb:
        h_dfs = {k.split("|")[0]: v
                 for k, v in st.session_state.ph_cache.items()
                 if not v.empty}
        st.download_button("⬇️ Excel（含 Price History）",
            data=to_excel(cart_df[show_c], h_dfs),
            file_name=f"cart_{ts}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True)
    with cc:
        if st.button("🗑️ 清空購物車", use_container_width=True):
            st.session_state.cart = []
            st.rerun()
