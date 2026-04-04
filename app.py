"""
Deutsche Börse Live Bond Search
Features: 搜尋、篩選、Yield、排序、購物車
"""

import streamlit as st
import requests
import pandas as pd
import hashlib
from datetime import datetime, timedelta, timezone
from io import BytesIO
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor, as_completed

SALT      = "af5a8d16eb5dc49f8a72b26fd9185475c7a"
BASE_URL  = "https://api.live.deutsche-boerse.com"
TZ_OFFSET = timezone(timedelta(hours=8))
MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun",
               "Jul","Aug","Sep","Oct","Nov","Dec"]

# ── Headers ──────────────────────────────────────────────────────────────────

def _make_headers(full_url):
    now         = datetime.now(tz=TZ_OFFSET)
    client_date = now.strftime("%Y-%m-%dT%H:%M:%S+08:00")
    x_security  = hashlib.md5(now.strftime("%Y%m%d%H%M").encode()).hexdigest()
    x_traceid   = hashlib.md5((client_date + full_url + SALT).encode()).hexdigest()
    return {
        "Accept":           "application/json, text/plain, */*",
        "Accept-Language":  "en-US,en;q=0.9",
        "Origin":           "https://live.deutsche-boerse.com",
        "Referer":          "https://live.deutsche-boerse.com/",
        "User-Agent":       "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "client-date":      client_date,
        "x-security":       x_security,
        "x-client-traceid": x_traceid,
        "content-type":     "application/json",
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
    return {}

# ── API ───────────────────────────────────────────────────────────────────────

def search_bonds(query, page=1, page_size=30):
    return _get("/v1/global_search/pagedsearch/bond/en",
                {"searchTerms": query, "page": page, "pageSize": page_size})

def get_master_data(isin):
    return _get("/v1/data/master_data_bond", {"isin": isin})

def get_trading_params(isin):
    return _get("/v1/data/frankfurt_trading_parameter", {"isin": isin})

def get_interest_rate_widget(isin):
    return _get("/v1/data/interest_rate_widget", {"isin": isin})

def get_last_price(isin, mic="XFRA"):
    end   = datetime.now(tz=TZ_OFFSET).strftime("%Y-%m-%d")
    start = (datetime.now(tz=TZ_OFFSET) - timedelta(days=10)).strftime("%Y-%m-%d")
    data  = _get("/v1/data/price_history",
                 {"isin": isin, "mic": mic, "minDate": start, "maxDate": end,
                  "limit": 5, "offset": 0}, timeout=20)
    rows  = data.get("data") or data.get("priceHistory") or []
    if not rows:
        return "—"
    rows_sorted = sorted(rows, key=lambda r: r.get("date", ""), reverse=True)
    return rows_sorted[0].get("close", "—")

def parse_maturity_year(query: str):
    import re
    m = re.search(r'\b\d{2}/(\d{2})\b', query)
    if m:
        yy = int(m.group(1))
        return 2000 + yy
    return None
    
def get_price_history(isin, start, end, mic="XFRA"):
    for attempt in range(2):
        try:
            return _get("/v1/data/price_history",
                        {"isin": isin, "mic": mic, "minDate": start, "maxDate": end,
                         "limit": 500, "offset": 0}, timeout=45)
        except requests.exceptions.Timeout:
            if attempt == 1:
                raise
        except requests.exceptions.HTTPError as e:
            # 500 = 該日無交易，回傳空資料即可
            if e.response is not None and e.response.status_code == 500:
                return {}
            raise
    return {}

# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe(d, *keys, default="—"):
    for k in keys:
        if not isinstance(d, dict): return default
        d = d.get(k)
        if d is None: return default
    return d if d not in (None, "", [], {}) else default

def _tr(field, default="—"):
    if isinstance(field, dict):
        return (field.get("translations", {}).get("en")
                or field.get("translations", {}).get("others")
                or field.get("originalValue") or default)
    return str(field) if field not in (None, "") else default

def _name(bond):
    for key in ("name", "instrumentName"):
        f = bond.get(key)
        if isinstance(f, dict):
            return (f.get("translations", {}).get("en") or f.get("originalValue") or "—")
        if isinstance(f, str) and f: return f
    return "—"

def calc_coupon_months(first_pay_date, cycle_orig, cycle_en):
    """
    cycle_orig: originalValue from API e.g. "2"
    cycle_en:   translations.en e.g. "semiannual"
    以 translations.en 為主判斷，originalValue 僅備用
    """
    if not first_pay_date or first_pay_date == "—": return "—"
    try:
        month = int(str(first_pay_date)[5:7])
    except:
        return "—"

    cv = str(cycle_en).lower().strip()
    # 用英文翻譯判斷
    if "monthly" in cv:        interval = 1
    elif "quarterly" in cv:    interval = 3
    elif "semi" in cv:         interval = 6
    elif "annual" in cv or "yearly" in cv: interval = 12
    else:
        # fallback: 用 originalValue（需要從實際資料確認，先做保守處理）
        ov = str(cycle_orig).strip()
        mapping = {"1": 12, "2": 6, "3": 3, "4": 1}  # 待確認，先保留
        interval = mapping.get(ov, 0)
        if interval == 0:
            return f"({cycle_en or cycle_orig})"

    count   = 12 // interval
    months  = sorted({((month - 1 + i * interval) % 12) + 1 for i in range(count)})
    return ", ".join(MONTH_NAMES[m - 1] for m in months)

def fetch_one(bond):
    isin    = _safe(bond, "isin")
    results = {}
    def call(name, fn, *args):
        try:    results[name] = fn(*args)
        except Exception as e: results[name] = {"_error": str(e)}
    with ThreadPoolExecutor(max_workers=4) as ex:
        fs = [ex.submit(call, "master",     get_master_data,          isin),
              ex.submit(call, "trading",    get_trading_params,       isin),
              ex.submit(call, "last_price", get_last_price,           isin),
              ex.submit(call, "interest",   get_interest_rate_widget, isin)]
        for f in as_completed(fs): pass
    return isin, bond, results

def build_row(isin, bond_hit, master, trading, last_price, interest):
    first_pay  = _safe(interest, "firstAnnualPayDate") or _safe(master, "firstAnnualPayDate")
    cycle_raw  = _safe(interest, "interestPaymentCycle", default={})
    cycle_orig = cycle_raw.get("originalValue", "") if isinstance(cycle_raw, dict) else str(cycle_raw)
    cycle_en   = (cycle_raw.get("translations", {}).get("en", "")
                  if isinstance(cycle_raw, dict) else "")
    coupon_mo  = calc_coupon_months(first_pay, cycle_orig, cycle_en)
    cycle_val  = cycle_en or cycle_orig or "—"

    lp = last_price if not isinstance(last_price, dict) else "—"

    # Yield = Coupon / Last Price * 100（當期年度殖利率）
    coupon = _safe(master, "cupon")
    yld = "—"
    try:
        if lp != "—" and coupon != "—":
            yld = round(float(coupon) / float(lp) * 100, 4)
    except:
        yld = "—"

    return {
        "ISIN":                    isin,
        "名稱":                    _name(bond_hit),
        "Last Price":              lp,
        "Coupon (%)":              _safe(master, "cupon"),
        "Yield (%)":               yld,
        "Maturity Date":           _safe(master, "maturity"),
        "First Interest Pay Date": first_pay,
        "Interest Payment Cycle":  cycle_val,
        "Coupon Months":           coupon_mo,
        "Issue Currency":          _safe(master, "issueCurrency"),
        "Min. Investment Amount":  _safe(master, "minimumInvestmentAmount"),
        "Min. Tradeable Unit":     _safe(trading, "minimumTradableUnit"),
        "Subordinated":            _safe(master, "subordinated"),
        "Trading Model":           _tr(_safe(trading, "tradingModel", default={})),
    }

def build_history_df(hist):
    rows = hist.get("data") or hist.get("priceHistory") or []
    if not rows: return None
    df = pd.DataFrame(rows)
    rename = {"date":"Date","open":"Open","close":"Close","high":"High","low":"Low",
              "turnoverPieces":"Volume","turnoverEuro":"Turnover (EUR)"}
    df.rename(columns={k:v for k,v in rename.items() if k in df.columns}, inplace=True)
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"])
        df.sort_values("Date", ascending=False, inplace=True)
        df.reset_index(drop=True, inplace=True)
    return df

def to_excel(rows_df, history_dfs):
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        rows_df.to_excel(writer, sheet_name="Summary", index=False)
        for isin, hdf in history_dfs.items():
            hdf.to_excel(writer, sheet_name=isin[:31], index=False)
    return buf.getvalue()

DISPLAY_COLS = [
    "ISIN", "名稱", "Last Price",
    "Coupon (%)", "Yield (%)",
    "Maturity Date", "First Interest Pay Date",
    "Interest Payment Cycle", "Coupon Months",
    "Issue Currency", "Min. Investment Amount", "Min. Tradeable Unit",
    "Subordinated", "Trading Model",
]

# ── UI ────────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="Börse Frankfurt Bond Search", page_icon="🏦", layout="wide")
st.title("🏦 Börse Frankfurt — Bond Search")
st.caption("資料來源：live.deutsche-boerse.com（延遲 ~15 分鐘）")

# Session state
for k, v in [("result_rows", []), ("history_cache", {}),
             ("ph_start", None), ("ph_end", None),
             ("ph_selected", []), ("cart", [])]:
    if k not in st.session_state:
        st.session_state[k] = v

# ── 搜尋列 ───────────────────────────────────────────────────────────────────
c1, c2, c3 = st.columns([4, 1, 1])
with c1:
    query = st.text_input("搜尋",
        placeholder="例如：25/55  |  EDF  |  Microsoft  |  US594918AM64",
        label_visibility="collapsed")
with c2:
    page_size = st.selectbox("最多筆數", [20, 50, 100, 200], index=1, label_visibility="collapsed")
with c3:
    do_search = st.button("🔍 搜尋", type="primary", use_container_width=True)

with st.expander("📅 Price History 日期範圍", expanded=False):
    d1, d2 = st.columns(2)
    with d1:
        ph_start = st.date_input("開始日期", value=datetime.today() - timedelta(days=365))
    with d2:
        ph_end = st.date_input("結束日期", value=datetime.today())

# ── 執行搜尋 ─────────────────────────────────────────────────────────────────
if do_search and query.strip():
    st.session_state.history_cache = {}
    st.session_state.ph_selected   = []
    with st.spinner(f"搜尋「{query}」並載入所有資料..."):
        try:
            raw  = search_bonds(query.strip(), page_size=page_size)
hits = raw.get("result") or raw.get("data") or []
total = raw.get("total") or len(hits)

target_year = parse_maturity_year(query.strip())
if target_year and hits:
    before = len(hits)
    yy = str(target_year)[2:]
    hits = [b for b in hits
            if f"/{yy}" in _name(b) or "/" not in _name(b)]
    if len(hits) < before:
        st.caption(f"ℹ️ 已自動移除非 {target_year} 年到期的結果（移除 {before - len(hits)} 筆）")

            if not hits:
                st.warning("找不到結果，請換個關鍵字。")
                st.session_state.result_rows = []
            else:
                prog = st.progress(0, text="載入中...")
                rows = []
                isin_order = [_safe(b, "isin") for b in hits]
                done_n = [0]

                with ThreadPoolExecutor(max_workers=5) as ex:
                    futures = {ex.submit(fetch_one, b): b for b in hits}
                    for f in as_completed(futures):
                        isin, bond, d = f.result()
                        master     = d.get("master",     {})
                        trading    = d.get("trading",    {})
                        last_price = d.get("last_price", "—")
                        interest   = d.get("interest",   {})
                        if isinstance(last_price, dict) and "_error" in last_price:
                            last_price = "—"
                        row = build_row(isin, bond, master, trading,
                                        last_price, interest)
                        errs = [d[k]["_error"] for k in ("master","trading","interest")
                                if isinstance(d.get(k), dict) and "_error" in d.get(k,{})]
                        if errs: row["_error"] = " | ".join(errs)
                        rows.append(row)
                        done_n[0] += 1
                        prog.progress(done_n[0]/len(hits), text=f"載入中 {done_n[0]}/{len(hits)}")

                prog.empty()
                rows.sort(key=lambda r: isin_order.index(r["ISIN"])
                          if r["ISIN"] in isin_order else 999)
                st.session_state.result_rows = rows
                st.session_state.ph_start    = ph_start
                st.session_state.ph_end      = ph_end
                st.rerun()
        except Exception as e:
            st.error(f"❌ 搜尋失敗：{e}")

# ── 顯示結果 ─────────────────────────────────────────────────────────────────
rows = st.session_state.result_rows
if rows:
    df_all = pd.DataFrame(rows)
    st.success(f"共載入 **{len(rows)}** 筆")

    # ── 篩選區 ────────────────────────────────────────────────────────────
    with st.expander("🔽 篩選條件", expanded=True):
        f1, f2, f3 = st.columns(3)

        with f1:
            all_currencies = sorted(df_all["Issue Currency"].dropna().unique().tolist())
            sel_currencies = st.multiselect("Issue Currency",
                options=all_currencies, default=all_currencies,
                key="f_currency")

        with f2:
            all_months = []
            for cm in df_all["Coupon Months"].dropna():
                for m in str(cm).split(", "):
                    if m.strip() and m.strip() != "—" and m.strip() not in all_months:
                        all_months.append(m.strip())
            all_months = sorted(all_months,
                key=lambda x: MONTH_NAMES.index(x) if x in MONTH_NAMES else 99)
            sel_months = st.multiselect("配息月份 (Coupon Months)",
                options=all_months, default=[],
                key="f_months")

        with f3:
            valid_coupons = pd.to_numeric(df_all["Coupon (%)"], errors="coerce").dropna()
            if not valid_coupons.empty:
                c_min, c_max = float(valid_coupons.min()), float(valid_coupons.max())
                if c_min < c_max:
                    coupon_range = st.slider("Coupon (%) 範圍",
                        min_value=c_min, max_value=c_max,
                        value=(c_min, c_max), step=0.1,
                        format="%.1f%%", key="f_coupon")
                else:
                    coupon_range = (c_min, c_max)
                    st.caption(f"Coupon: {c_min}%")
            else:
                coupon_range = (0.0, 99.0)

        f4, f5 = st.columns(2)

        with f4:
            # Subordinated 篩選
            sub_options = ["全部", "❌ 否 (Non-subordinated)", "✅ 是 (Subordinated)"]
            sel_sub = st.radio("Subordinated", options=sub_options,
                               index=0, horizontal=True, key="f_sub")

        with f5:
            # Min Investment Amount 上限篩選
            valid_mia = pd.to_numeric(df_all["Min. Investment Amount"], errors="coerce").dropna()
            if not valid_mia.empty:
                mia_max = float(valid_mia.max())
                mia_limit = st.number_input(
                    "Min. Investment Amount 上限",
                    min_value=0.0, max_value=mia_max,
                    value=mia_max, step=1000.0,
                    format="%.0f",
                    key="f_mia",
                    help="只顯示最低投資金額 ≤ 此值的債券"
                )
            else:
                mia_limit = float("inf")

    # 套用篩選
    df = df_all.copy()
    if sel_currencies:
        df = df[df["Issue Currency"].isin(sel_currencies)]
    if sel_months:
        def has_month(cm):
            if not cm or cm == "—": return False
            return any(m in str(cm).split(", ") for m in sel_months)
        df = df[df["Coupon Months"].apply(has_month)]
    df["_coupon_num"] = pd.to_numeric(df["Coupon (%)"], errors="coerce")
    df = df[df["_coupon_num"].between(coupon_range[0], coupon_range[1]) |
            df["_coupon_num"].isna()]
    # Subordinated 篩選
    if sel_sub == "❌ 否 (Non-subordinated)":
        df = df[df["Subordinated"].apply(lambda x: x is False or str(x).lower() in ("false","no","❌ 否","—","none","null"))]
    elif sel_sub == "✅ 是 (Subordinated)":
        df = df[df["Subordinated"].apply(lambda x: x is True or str(x).lower() in ("true","yes","✅ 是"))]
    # Min Investment Amount 篩選
    df["_mia_num"] = pd.to_numeric(df["Min. Investment Amount"], errors="coerce")
    df = df[df["_mia_num"].isna() | (df["_mia_num"] <= mia_limit)]

    # ── 排序：Currency → Coupon Month (首月) → Maturity Date ──────────────
    def sort_key(row):
        curr = str(row.get("Issue Currency", "zzz"))
        cm   = str(row.get("Coupon Months", ""))
        first_month = cm.split(", ")[0] if cm and cm != "—" else "zzz"
        mo_idx = MONTH_NAMES.index(first_month) if first_month in MONTH_NAMES else 99
        mat  = str(row.get("Maturity Date", "9999"))
        return (curr, mo_idx, mat)

    df_sorted = pd.DataFrame(sorted(df.to_dict("records"), key=sort_key))

    # ── 只顯示打勾項目 toggle ────────────────────────────────────────────
    cart_isins = {r["ISIN"] for r in st.session_state.cart}
    col_cap, col_toggle = st.columns([3, 1])
    with col_cap:
        st.caption(f"篩選後：**{len(df_sorted)}** 筆　|　購物車：**{len(cart_isins)}** 筆")
    with col_toggle:
        show_cart_only = st.toggle("只顯示打勾項目", value=False, key="show_cart_only")

    if show_cart_only and cart_isins:
        df_sorted = df_sorted[df_sorted["ISIN"].isin(cart_isins)]

    # ── 單日模式：把已載入的該日價格加進主表 ────────────────────────────
    _start = st.session_state.get("ph_start") or ph_start
    _end   = st.session_state.get("ph_end")   or ph_end
    is_single_day = str(_start) == str(_end)

    show_cols = [c for c in DISPLAY_COLS if c in df_sorted.columns]

    if is_single_day:
        # 單日模式：顯示兩欄，從 history_cache 回填已載入的價格
        df_sorted = df_sorted.copy()
        df_sorted["📅 選擇日期"] = str(_start)
        day_prices = {}
        for isin, hdf in st.session_state.history_cache.items():
            if hdf is not None and not hdf.empty and "Close" in hdf.columns:
                # 取最接近指定日期的那筆
                day_prices[isin] = hdf.iloc[0]["Close"]
        df_sorted["💰 該日價格"] = df_sorted["ISIN"].map(day_prices)
        show_cols = ["ISIN", "名稱", "📅 選擇日期", "💰 該日價格"] + \
                    [c for c in show_cols if c not in ("ISIN", "名稱")]
    # ── 主表格（唯讀）+ 購物車 checkbox ─────────────────────────────────
    show_cols_no_isin = [c for c in show_cols if c != "ISIN"]

    # header
    header_cols = st.columns([0.5, 1.5] + [2] * len(show_cols_no_isin))
    header_cols[0].markdown("**🛒**")
    header_cols[1].markdown("**ISIN**")
    for ci, col_name in enumerate(show_cols_no_isin):
        header_cols[ci + 2].markdown(f"**{col_name}**")

    st.markdown("---")

    rows_dict = df_sorted.to_dict("records")
    for row in rows_dict:
        isin = row["ISIN"]
        cb_key = f"cart_cb_{isin}"
        # 初始化 checkbox state（只有第一次）
        if cb_key not in st.session_state:
            st.session_state[cb_key] = isin in cart_isins

        row_cols = st.columns([0.5, 1.5] + [2] * len(show_cols_no_isin))

        # Checkbox
        checked = row_cols[0].checkbox("", key=cb_key, label_visibility="collapsed")

        # 同步購物車
        if checked and isin not in cart_isins:
            st.session_state.cart.append(row)
            cart_isins.add(isin)
        elif not checked and isin in cart_isins:
            st.session_state.cart = [r for r in st.session_state.cart if r["ISIN"] != isin]
            cart_isins.discard(isin)

        # ISIN
        row_cols[1].write(isin)

        # 其他欄位
        for ci, col_name in enumerate(show_cols_no_isin):
            val = row.get(col_name, "—")
            if val is None or val == "" or (isinstance(val, float) and pd.isna(val)):
                val = "—"
            if isinstance(val, float):
                if col_name in ("Last Price", "💰 該日價格"):
                    val = f"{val:.4f}"
                elif col_name in ("Coupon (%)", "Yield (%)"):
                    val = f"{val:.3f}%"
                elif col_name == "Min. Investment Amount":
                    val = f"{val:,.0f}"
                else:
                    val = f"{val}"
            row_cols[ci + 2].write(str(val))

    # ── Price History ─────────────────────────────────────────────────────
    st.divider()
    st.subheader("📈 Price History")
    names     = {r["ISIN"]: r["名稱"] for r in rows}
    isin_list = [r["ISIN"] for r in rows]

    # 購物車裡的 ISIN 自動加進 ph_selected
    cart_isin_list = [r["ISIN"] for r in st.session_state.cart if r["ISIN"] in isin_list]
    prev_selected  = [i for i in st.session_state.ph_selected if i in isin_list]
    # 合併去重，確保購物車的都在裡面
    combined = list(dict.fromkeys(cart_isin_list + prev_selected))
    # 只有在有新的購物車項目時才更新（避免覆蓋用戶手動取消的選項）
    if set(cart_isin_list) - set(st.session_state.ph_selected):
        st.session_state.ph_selected = combined
        st.session_state["ph_multiselect"] = combined

    selected = st.multiselect(
        "選擇要查看 Price History 的債券",
        options=isin_list,
        default=[i for i in st.session_state.ph_selected if i in isin_list],
        format_func=lambda x: f"{x} — {names.get(x,'')}",
        key="ph_multiselect",
    )
    st.session_state.ph_selected = selected

    if selected:
        if st.button("📥 載入 Price History", type="primary"):
            prog2 = st.progress(0)
            done2 = [0]

            def load_one_history(isin):
                try:
                    hist = get_price_history(isin,
                                             _start.strftime("%Y-%m-%d"),
                                             _end.strftime("%Y-%m-%d"))
                    return isin, build_history_df(hist), None
                except Exception as e:
                    return isin, pd.DataFrame(), str(e)

            with ThreadPoolExecutor(max_workers=4) as ex:
                fut_map = {ex.submit(load_one_history, isin): isin for isin in selected}
                for f in as_completed(fut_map):
                    isin, hdf, err = f.result()
                    st.session_state.history_cache[isin] = hdf
                    done2[0] += 1
                    prog2.progress(done2[0] / len(selected),
                                   text=f"載入中 {done2[0]}/{len(selected)} — {isin}")
                    if err and "timeout" in err.lower():
                        st.warning(f"{isin}：連線逾時，請稍後再試")
            prog2.empty()

    # 單日判斷：統一轉成字串再比較，避免 date vs str 型別問題
    is_single_day = str(_start) == str(_end)

    for isin in selected:
        if isin not in st.session_state.history_cache: continue
        hdf  = st.session_state.history_cache[isin]
        name = names.get(isin, isin)

        with st.expander(f"**{name}** `{isin}`", expanded=True):
            if hdf is not None and not hdf.empty:
                if is_single_day:
                    if "Close" in hdf.columns:
                        close_val = hdf.iloc[0]["Close"]
                        date_val  = hdf.iloc[0]["Date"].strftime("%Y-%m-%d") if "Date" in hdf.columns else str(_start)
                        c1, c2 = st.columns(2)
                        with c1:
                            st.metric("📅 選擇日期", date_val)
                        with c2:
                            st.metric("💰 該日價格 (Close)", f"{close_val:.4f}")
                    else:
                        st.info("無 Close 資料")
                else:
                    st.markdown(f"{_start} ～ {_end}　共 **{len(hdf)}** 筆")
                    if "Close" in hdf.columns and "Date" in hdf.columns:
                        st.line_chart(hdf.sort_values("Date").set_index("Date")[["Close"]], height=200)
                    st.dataframe(hdf, use_container_width=True, hide_index=True)
            else:
                st.info("此期間無資料")

# ── 購物車區 ─────────────────────────────────────────────────────────────────
st.divider()
st.subheader("🛒 購物車")

cart = st.session_state.cart
if not cart:
    st.caption("尚未加入任何債券，在上方表格的 🛒 欄位打勾即可加入。")
else:
    st.success(f"已選 **{len(cart)}** 筆")
    cart_df = pd.DataFrame(cart)
    show_cart_cols = [c for c in DISPLAY_COLS if c in cart_df.columns]
    st.dataframe(cart_df[show_cart_cols], use_container_width=True, hide_index=True,
                 column_config={
                     "Last Price": st.column_config.NumberColumn(format="%.4f"),
                     "Coupon (%)": st.column_config.NumberColumn(format="%.3f%%"),
                     "Yield (%)":  st.column_config.NumberColumn(format="%.4f%%"),
                 })

    ca, cb, cc, _ = st.columns([2, 2, 2, 2])
    ts = datetime.today().strftime("%Y%m%d_%H%M")
    with ca:
        st.download_button("⬇️ CSV 匯出購物車",
            data=cart_df[show_cart_cols].to_csv(index=False).encode("utf-8-sig"),
            file_name=f"cart_{ts}.csv", mime="text/csv",
            type="primary", use_container_width=True)
    with cb:
        h_dfs = {k: v for k, v in st.session_state.history_cache.items()
                 if v is not None and not v.empty}
        st.download_button("⬇️ Excel 匯出（含 Price History）",
            data=to_excel(cart_df[show_cart_cols], h_dfs),
            file_name=f"cart_{ts}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True)
    with cc:
        if st.button("🗑️ 清空購物車", use_container_width=True):
            st.session_state.cart = []
            st.rerun()
