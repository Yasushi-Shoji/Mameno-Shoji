# =========================================================
# JMA（気象庁）過去の気象データ検索から
# 47都道府県庁所在地：2025年6-9月（JJAS）
# 平均気温・平年値(1991-2020)・平年差
# ＋ 猛暑日/真夏日/熱帯夜（6-9月合計）
# ＋ WBGT簡易（気温+湿度から推定：6-9月）
#
# 出力:
#  - jma_prefcap_codes.csv
#  - summer_temp_2025_prefcap_JJAS.csv
#  - summer_2025_prefcap_final_JJAS.csv
#  - report_JJAS.qmd (+ quartoがあれば report_JJAS.html)
# =========================================================

# ---------- packages ----------
pkgs <- c("rvest","dplyr","stringr","purrr","readr","tibble","tidyr","ggplot2","httr")
need <- pkgs[!pkgs %in% rownames(installed.packages())]
if(length(need) > 0) install.packages(need)
invisible(lapply(pkgs, library, character.only = TRUE))

# ---------- polite access settings ----------
RATE_BASE_SEC <- 0.8      # 基本待機（秒）: 0.8秒
RATE_JITTER_SEC <- 0.4    # 追加ゆらぎ（0～0.4秒）
MAX_RETRY <- 3
RETRY_BACKOFF <- 1.5      # リトライ時の指数バックオフ係数

ua <- httr::user_agent("Mozilla/5.0 (compatible; research-script/1.0; +https://www.data.jma.go.jp/)")

polite_sleep <- function() {
  Sys.sleep(RATE_BASE_SEC + runif(1, 0, RATE_JITTER_SEC))
}

safe_read_html <- function(url) {
  # 待機＋リトライ＋UA付与
  for (i in seq_len(MAX_RETRY)) {
    polite_sleep()
    res <- tryCatch({
      httr::GET(url, ua, httr::timeout(30))
    }, error = function(e) NULL)
    
    if (is.null(res)) {
      Sys.sleep((RETRY_BACKOFF^i))
      next
    }
    
    sc <- httr::status_code(res)
    if (sc >= 200 && sc < 300) {
      txt <- httr::content(res, as = "text", encoding = "UTF-8")
      return(read_html(txt))
    } else {
      # 429/5xx等を想定して待ってリトライ
      Sys.sleep((RETRY_BACKOFF^i))
    }
  }
  stop("Failed to fetch after retries: ", url)
}

# ---------- constants ----------
YEAR <- 2025
MONTHS <- 6:9  # JJAS
WBGT_THRESHOLD <- 28

# ---------- 47 capitals list (JMA表記を吸収) ----------
capitals <- tibble::tribble(
  ~pref, ~capital, ~capital_name_jma,
  "北海道","札幌","札幌",
  "青森県","青森","青森",
  "岩手県","盛岡","盛岡",
  "宮城県","仙台","仙台",
  "秋田県","秋田","秋田",
  "山形県","山形","山形",
  "福島県","福島","福島",
  "茨城県","水戸","水戸",
  "栃木県","宇都宮","宇都宮",
  "群馬県","前橋","前橋",
  "埼玉県","さいたま","さいたま",
  "千葉県","千葉","千葉",
  "東京都","東京","東京",
  "神奈川県","横浜","横浜",
  "新潟県","新潟","新潟",
  "富山県","富山","富山",
  "石川県","金沢","金沢",
  "福井県","福井","福井",
  "山梨県","甲府","甲府",
  "長野県","長野","長野",
  "岐阜県","岐阜","岐阜",
  "静岡県","静岡","静岡",
  "愛知県","名古屋","名古屋",
  "三重県","津","津",
  "滋賀県","大津","大津",
  "京都府","京都","京都",
  "大阪府","大阪","大阪",
  "兵庫県","神戸","神戸",
  "奈良県","奈良","奈良",
  "和歌山県","和歌山","和歌山",
  "鳥取県","鳥取","鳥取",
  "島根県","松江","松江",
  "岡山県","岡山","岡山",
  "広島県","広島","広島",
  "山口県","山口","山口",
  "徳島県","徳島","徳島",
  "香川県","高松","高松",
  "愛媛県","松山","松山",
  "高知県","高知","高知",
  "福岡県","福岡","福岡",
  "佐賀県","佐賀","佐賀",
  "長崎県","長崎","長崎",
  "熊本県","熊本","熊本",
  "大分県","大分","大分",
  "宮崎県","宮崎","宮崎",
  "鹿児島県","鹿児島","鹿児島",
  "沖縄県","那覇","那覇"
) %>% mutate(pref_alt = pref)

# ---------- Step 1: get prec_no list ----------
pref_select_url <- "https://www.data.jma.go.jp/stats/etrn/select/prefecture00"
pref0 <- safe_read_html(pref_select_url)

pref_links <- pref0 %>%
  html_elements("area") %>%
  { tibble::tibble(
    alt  = rvest::html_attr(., "alt"),
    href = rvest::html_attr(., "href")
  )
  } %>%
  dplyr::filter(!is.na(href)) %>%
  dplyr::mutate(
    prec_no = stringr::str_match(href, "prec_no=([0-9]+)")[, 2],
    prec_no = as.integer(prec_no)
  ) %>%
  dplyr::filter(!is.na(prec_no)) %>%
  dplyr::distinct(alt, prec_no)

# ---------- Step 2: get stations per prec_no (block_no, station name) ----------
get_stations_in_pref <- function(prec_no){
  url <- sprintf(
    "https://www.data.jma.go.jp/stats/etrn/select/prefecture.php?prec_no=%d&block_no=&year=&month=&day=&view=",
    prec_no
  )
  pg <- safe_read_html(url)
  
  tibble::tibble(
    station = pg %>% rvest::html_elements("area") %>% rvest::html_attr("alt"),
    href    = pg %>% rvest::html_elements("area") %>% rvest::html_attr("href")
  ) %>%
    dplyr::filter(!is.na(href), !is.na(station)) %>%
    dplyr::mutate(
      block_no = stringr::str_match(href, "block_no=([0-9]+)")[,2],
      block_no = as.integer(block_no)
    ) %>%
    dplyr::filter(!is.na(block_no)) %>%
    dplyr::select(block_no, station) %>%
    dplyr::distinct()
}

all_stations <- pref_links %>%
  dplyr::transmute(prec_no) %>%
  dplyr::distinct() %>%
  dplyr::mutate(st = purrr::map(prec_no, get_stations_in_pref)) %>%
  tidyr::unnest(st)

# ---------- Step 3: match prefecture-capital to station to get codes ----------
capital_codes <- capitals %>%
  left_join(pref_links, by = c("pref_alt" = "alt")) %>%
  left_join(all_stations, by = "prec_no") %>%
  filter(station == capital_name_jma) %>%
  transmute(pref, capital, capital_name_jma, prec_no, block_no) %>%
  arrange(pref) %>%
  mutate(
    etrn_index_url = sprintf("https://www.data.jma.go.jp/stats/etrn/index.php?prec_no=%d&block_no=%d", prec_no, block_no)
  )

if (nrow(capital_codes) != 47) {
  warning("取得できた県庁所在地が47件ではありません。表記ゆれ等でマッチしない県がある可能性があります。\n",
          "n = ", nrow(capital_codes), "\n",
          "不足確認用：", paste(setdiff(capitals$pref, capital_codes$pref), collapse = ", "))
}

write_csv(capital_codes, "jma_prefcap_codes.csv")

# ---------- Step 4: monthly mean temperature for YEAR ----------
get_monthly_table_s3 <- function(prec_no, block_no){
  url <- sprintf(
    "https://www.data.jma.go.jp/stats/etrn/view/monthly_s3.php?prec_no=%d&block_no=%d",
    prec_no, block_no
  )
  pg <- safe_read_html(url)
  tabs <- pg %>% rvest::html_table(fill = TRUE)
  
  if (length(tabs) == 0) return(NULL)
  
  ok <- which(purrr::map_lgl(tabs, function(tb){
    nms <- names(tb)
    length(nms) >= 13 && all(as.character(1:12) %in% nms)
  }))
  
  if (length(ok) == 0) return(NULL)
  
  tb <- tabs[[ok[1]]]
  names(tb)[1] <- "year"
  tb$year <- suppressWarnings(as.integer(tb$year))
  tb
}

get_monthly_table_s1 <- function(prec_no, block_no, year){
  url <- sprintf(
    "https://www.data.jma.go.jp/stats/etrn/view/monthly_s1.php?prec_no=%d&block_no=%d&year=%d&month=&day=&view=",
    prec_no, block_no, year
  )
  pg <- safe_read_html(url)
  tabs <- pg %>% rvest::html_table(fill = TRUE)
  
  if (length(tabs) == 0) return(NULL)
  
  ok <- which(purrr::map_lgl(tabs, function(tb){
    nms <- names(tb)
    length(nms) >= 13 && all(as.character(1:12) %in% nms)
  }))
  
  if (length(ok) == 0) return(NULL)
  
  tb <- tabs[[ok[1]]]
  names(tb)[1] <- "year"
  tb$year <- suppressWarnings(as.integer(tb$year))
  tb
}

get_obs_months <- function(prec_no, block_no, year, months){
  tb <- get_monthly_table_s3(prec_no, block_no)
  if (is.null(tb)) tb <- get_monthly_table_s1(prec_no, block_no, year)
  if (is.null(tb)) return(rep(NA_real_, length(months)))
  
  row <- tb %>% dplyr::filter(year == !!year)
  if (nrow(row) == 0) return(rep(NA_real_, length(months)))
  
  purrr::map_dbl(months, function(m){
    suppressWarnings(as.numeric(row[[as.character(m)]]))
  })
}

# ---------- Step 5: normals (1991-2020) for months ----------
safe_read_html <- function(url) {
  for (i in seq_len(MAX_RETRY)) {
    polite_sleep()
    res <- tryCatch(httr::GET(url, ua, httr::timeout(30)), error = function(e) NULL)
    if (is.null(res)) {
      Sys.sleep(RETRY_BACKOFF^i)
      next
    }
    sc <- httr::status_code(res)
    if (sc >= 200 && sc < 300) {
      raw <- httr::content(res, as = "raw")
      return(xml2::read_html(raw))
    }
    Sys.sleep(RETRY_BACKOFF^i)
  }
  stop("Failed to fetch: ", url)
}

to_halfwidth_num <- function(x) {
  x <- as.character(x)
  x <- chartr("０１２３４５６７８９", "0123456789", x)
  x
}

as_num <- function(x) {
  x <- to_halfwidth_num(x)
  x <- gsub(",", "", x)
  suppressWarnings(as.numeric(gsub("[^0-9\\.-]", "", x)))
}

table_to_matrix <- function(tbl_node) {
  trs <- rvest::html_elements(tbl_node, "tr")
  rows <- lapply(trs, function(tr) {
    cells <- rvest::html_elements(tr, "th, td")
    txt <- rvest::html_text(cells, trim = TRUE)
    txt <- gsub("\\s+", "", txt)
    txt
  })
  maxlen <- max(lengths(rows))
  if (maxlen == 0) return(NULL)
  rows2 <- lapply(rows, function(r) { length(r) <- maxlen; r })
  m <- do.call(rbind, rows2)
  m
}

find_header_row <- function(m) {
  if (is.null(m)) return(NA_integer_)
  for (i in seq_len(nrow(m))) {
    r <- m[i, ]
    if (any(grepl("^1月$", r)) && any(grepl("^12月$", r))) return(i)
  }
  NA_integer_
}

find_mean_temp_row <- function(m) {
  if (is.null(m)) return(NA_integer_)
  
  hit <- which(apply(m, 1, function(r) {
    any(grepl("平均気温", r))
  }))
  if (length(hit) > 0) return(hit[1])
  
  hit2 <- which(apply(m, 1, function(r) {
    any(grepl("気温", r)) &&
      any(grepl("平均", r)) &&
      !any(grepl("最高", r)) &&
      !any(grepl("最低", r))
  }))
  if (length(hit2) > 0) return(hit2[1])
  
  NA_integer_
}
get_normal_months <- function(prec_no, block_no, months) {
  url <- sprintf("https://www.data.jma.go.jp/stats/etrn/view/nml_sfc_ym.php?prec_no=%d&block_no=%d",
                 prec_no, block_no)
  
  pg <- safe_read_html(url)
  tables <- rvest::html_elements(pg, "table")
  if (length(tables) == 0) return(rep(NA_real_, length(months)))
  
  for (tbl in tables) {
    m <- table_to_matrix(tbl)
    if (is.null(m)) next
    
    h <- find_header_row(m)
    if (is.na(h)) next
    
    header <- m[h, ]
    pos <- setNames(rep(NA_integer_, 12), as.character(1:12))
    for (k in 1:12) {
      idx <- which(header == paste0(k, "月"))
      if (length(idx) > 0) pos[as.character(k)] <- idx[1]
    }
    if (any(is.na(pos))) next
    
    r_idx <- find_mean_temp_row(m)
    if (is.na(r_idx)) next
    
    row <- m[r_idx, ]
    out <- map_dbl(months, function(mm) {
      j <- pos[as.character(mm)]
      if (is.na(j)) return(NA_real_)
      as_num(row[j])
    })
    return(out)
  }
  
  rep(NA_real_, length(months))
}
nan_to_na <- function(x) ifelse(is.nan(x), NA_real_, x)

summer_temp <- capital_codes %>%
  rowwise() %>%
  mutate(
    obs = list(tryCatch(get_obs_months(prec_no, block_no, YEAR, MONTHS),
                        error = function(e) rep(NA_real_, length(MONTHS))
    )),
    normal_m = list(tryCatch(get_normal_months(prec_no, block_no, MONTHS),
                             error = function(e) rep(NA_real_, length(MONTHS))
    )),
    mean_2025 = nan_to_na(mean(unlist(obs), na.rm = TRUE)),
    normal = nan_to_na(mean(unlist(normal_m), na.rm = TRUE)),
    anomaly = mean_2025 - normal
  ) %>%
  ungroup() %>%
  select(pref, capital, prec_no, block_no, mean_2025, normal, anomaly) %>%
  mutate(across(c(mean_2025, normal, anomaly), ~ round(.x, 1)))

write_csv(summer_temp, "summer_temp_2025_prefcap_JJAS.csv")

summer_temp %>% summarise(n = n(), normal_na = sum(is.na(normal)))
summer_temp %>% filter(is.na(normal)) %>% select(pref, capital, prec_no, block_no) %>% head(20)




# ---------- Step 6: hot-day categories (猛暑日/真夏日/熱帯夜) ----------
# ここは「2025年の気温区分日数」ページを利用
# 列名はページ側の表記ゆれがあるので、"夏(6～9月)" 相当の列を頑健に拾う

ctg_url <- "https://www.data.jma.go.jp/stats/stat/202515/tem_ctg_days_202515.html"
ctg_tabs <- safe_read_html(ctg_url) %>% html_table(fill = TRUE)

ctg_all <- bind_rows(ctg_tabs, .id = "tbl") %>%
  rename(station = 1) %>%
  filter(!is.na(station), station != "") %>%
  # 見出し行っぽいものを除く
  filter(!grepl("地\\s*点\\s*名|地点名|都道府県|県|府|都|道", station))

find_best_col <- function(df, patterns){
  hits <- names(df)[Reduce(`&`, lapply(patterns, function(p) grepl(p, names(df))))]
  if (length(hits) > 0) return(hits[1])
  # fallback: なるべく近い列
  hits2 <- names(df)[grepl(patterns[1], names(df))]
  if (length(hits2) > 0) return(hits2[1])
  NA_character_
}

# 6-9月（夏）列を狙う（"夏"と"9"が入ってる列が最優先）
hot_col  <- find_best_col(ctg_all, c("猛暑日", "夏", "9"))
trop_col <- find_best_col(ctg_all, c("真夏日", "夏", "9"))
night_col<- find_best_col(ctg_all, c("日最低気温25|熱帯夜", "夏", "9"))

# もし見つからない場合は "夏" だけで拾う
if (is.na(hot_col))  hot_col  <- find_best_col(ctg_all, c("猛暑日", "夏"))
if (is.na(trop_col)) trop_col <- find_best_col(ctg_all, c("真夏日", "夏"))
if (is.na(night_col))night_col<- find_best_col(ctg_all, c("日最低気温25|熱帯夜", "夏"))

ctg_prefcap <- ctg_all %>%
  transmute(
    station,
    moshobi = if(!is.na(hot_col)) as.character(.data[[hot_col]]) else NA_character_,
    manatsubi = if(!is.na(trop_col)) as.character(.data[[trop_col]]) else NA_character_,
    nattaiya = if(!is.na(night_col)) as.character(.data[[night_col]]) else NA_character_
  ) %>%
  mutate(
    moshobi_6_9 = as.integer(str_extract(moshobi, "^[0-9]+")),
    manatsubi_6_9 = as.integer(str_extract(manatsubi, "^[0-9]+")),
    nattaiya_6_9 = as.integer(str_extract(nattaiya, "^[0-9]+"))
  ) %>%
  select(station, moshobi_6_9, manatsubi_6_9, nattaiya_6_9)

joined <- summer_temp %>%
  left_join(ctg_prefcap, by = c("capital" = "station"))

# ---------- Step 7: WBGT (simple) from daily temp+RH ----------
wbgt_simple <- function(Ta, RH){
  # Ta: Celsius, RH: %
  e <- (RH/100) * 6.105 * exp(17.27 * Ta / (237.7 + Ta))  # hPa
  0.567 * Ta + 0.393 * e + 3.94
}

get_daily_a2 <- function(prec_no, block_no, year, month){
  
  url <- sprintf(
    "https://www.data.jma.go.jp/stats/etrn/view/daily_s1.php?prec_no=%d&block_no=%d&year=%d&month=%d&day=&view=a2",
    prec_no, block_no, year, month
  )
  
  pg <- safe_read_html(url)
  tabs <- pg %>% rvest::html_table(fill = TRUE)
  if (length(tabs) == 0) {
    return(tibble::tibble(day = integer(), Ta = numeric(), RH = numeric()))
  }
  
  tab <- tabs[[1]]
  
  nm <- names(tab)
  
  day_col <- nm[1]
  
  pick_col <- function(nm, include1, include2 = NULL, exclude = NULL){
    idx <- rep(TRUE, length(nm))
    idx <- idx & grepl(include1, nm)
    if (!is.null(include2)) idx <- idx & grepl(include2, nm)
    if (!is.null(exclude))  idx <- idx & !grepl(exclude, nm)
    hit <- nm[idx]
    if (length(hit) == 0) return(NA_character_)
    hit[1]
  }
  
  tmean_col <- pick_col(nm, "気温", "平均")
  if (is.na(tmean_col)) tmean_col <- pick_col(nm, "平均気温|気温.*平均", NULL)
  
  rhmean_col <- pick_col(nm, "湿度", "平均")
  if (is.na(rhmean_col)) rhmean_col <- pick_col(nm, "平均湿度|湿度.*平均", NULL)
  
  if (is.na(tmean_col) || is.na(rhmean_col)) {
    return(tibble::tibble(day = integer(), Ta = numeric(), RH = numeric()))
  }
  
  out <- tab %>%
    dplyr::transmute(
      day = suppressWarnings(as.integer(.data[[day_col]])),
      Ta  = suppressWarnings(as.numeric(.data[[tmean_col]])),
      RH  = suppressWarnings(as.numeric(.data[[rhmean_col]]))
    ) %>%
    dplyr::filter(!is.na(day))
  
  out
}

wbgt_summer <- capital_codes %>%
  rowwise() %>%
  mutate(
    daily = list({
      lst <- lapply(MONTHS, function(m){
        tryCatch(
          get_daily_a2(prec_no, block_no, YEAR, m),
          error = function(e) tibble::tibble(day = integer(), Ta = numeric(), RH = numeric())
        )
      })
      dplyr::bind_rows(lst)
    }),
    wbgt_mean_6_9 = {
      if (nrow(daily) == 0) NA_real_
      else mean(wbgt_simple(daily$Ta, daily$RH), na.rm = TRUE)
    },
    wbgt_days_ge_28_6_9 = {
      if (nrow(daily) == 0) NA_integer_
      else sum(wbgt_simple(daily$Ta, daily$RH) >= WBGT_THRESHOLD, na.rm = TRUE)
    }
  ) %>%
  ungroup() %>%
  select(pref, capital, wbgt_mean_6_9, wbgt_days_ge_28_6_9) %>%
  mutate(
    wbgt_mean_6_9 = round(wbgt_mean_6_9, 1)
  )

final <- joined %>%
  left_join(wbgt_summer, by = c("pref","capital"))

write_csv(final, "summer_2025_prefcap_final_JJAS.csv")

# ---------- Step 8: Quarto report template ----------
qmd <- c(
  "---",
  "title: \"2025年 夏（6–9月）：都道府県庁所在地の暑さ（気温・猛暑日・WBGT）\"",
  "format:",
  "  html:",
  "    toc: true",
  "    number-sections: true",
  "execute:",
  "  warning: false",
  "  message: false",
  "---",
  "",
  "```{r}",
  "library(readr); library(dplyr); library(ggplot2)",
  "df <- read_csv(\"summer_2025_prefcap_final_JJAS.csv\", show_col_types = FALSE)",
  "df",
  "```",
  "",
  "## 1) 平年差（平均気温）上位",
  "",
  "```{r}",
  "df %>% arrange(desc(anomaly)) %>% slice(1:15) %>%",
  "  select(pref, capital, mean_2025, normal, anomaly, moshobi_6_9, wbgt_mean_6_9)",
  "```",
  "",
  "## 2) 平年差（平均気温）ランキング",
  "",
  "```{r}",
  "df %>% arrange(anomaly) %>%",
  "  mutate(capital = factor(capital, levels = capital)) %>%",
  "  ggplot(aes(x = capital, y = anomaly)) +",
  "  geom_col() + coord_flip() +",
  "  labs(x = NULL, y = \"平均気温の平年差 (℃)\")",
  "```",
  "",
  "## 3) 猛暑日（6–9月合計）と平年差",
  "",
  "```{r}",
  "df %>%",
  "  ggplot(aes(x = anomaly, y = moshobi_6_9)) +",
  "  geom_point() +",
  "  geom_smooth(method = \"lm\", se = FALSE) +",
  "  labs(x = \"平均気温の平年差 (℃)\", y = \"猛暑日数（35℃以上, 6–9月）\")",
  "```",
  "",
  "## 4) WBGT（簡易）と猛暑日",
  "",
  "```{r}",
  "df %>%",
  "  ggplot(aes(x = wbgt_mean_6_9, y = moshobi_6_9)) +",
  "  geom_point() +",
  "  geom_smooth(method = \"lm\", se = FALSE) +",
  "  labs(x = \"平均WBGT（簡易, ℃, 6–9月）\", y = \"猛暑日数（6–9月）\")",
  "```"
)

writeLines(qmd, "report_JJAS.qmd")

# Render if quarto is available
if (nzchar(Sys.which("quarto"))) {
  message("Quarto found: rendering report_JJAS.qmd ...")
  system("quarto render report_JJAS.qmd", intern = FALSE)
} else {
  message("Quarto not found: report_JJAS.qmd を作成しました（必要なら Quarto を入れて render してください）")
}

message("DONE. Outputs written:")
message(" - jma_prefcap_codes.csv")
message(" - summer_temp_2025_prefcap_JJAS.csv")
message(" - summer_2025_prefcap_final_JJAS.csv")
message(" - report_JJAS.qmd (and report_JJAS.html if Quarto installed)")

