#!/usr/bin/env python3

import requests
import csv
from datetime import datetime, timedelta
import pandas as pd
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 설정 값
API_KEY = "YOUR_API_KEY_HERE"  # API 키를 여기에 입력하세요
OUTPUT_DIR = "economic_indicators_data"
SERIES_IDS = [
    ("UNRATE", "unemployment Ratio"),
    ("SAHMREALTIME", "Real-time Sahm Rule Recession Indicator"),
    ("ICSA", "Initial Claims"),
    ("USCONS", "All Employees, Construction"),
    ("JTSJOL", "Job Openings: Total Nonfarm"),
    ("DTWEXBGS", "Dollar index"),
    ("DCOILWTICO", "WTI Crude Oil"),
    ("CES4348400001", "All Employees, Truck Transportation"),
    ("M2SL", "M2"),
    ("M2V", "M2V"),
    ("GDPC1", "Real GDP"),
    ("AMTMNO", "PMI - New Orders"),
    ("BAMLH0A0HYM2EY", "High-Yield"),
    ("DGS10", "10 year treasury"),
    ("DGS2", "2 year treasury"),
    ("FEDFUNDS", "Federal Funds Effective Rate"),
    ("MORTGAGE30US", "30-Year Fixed Rate Mortgage Average in the United States"),
    ("CPIAUCSL", "Consumer Price Index for All Urban Consumers: All Items in U.S. City Average"),
    ("COMREPUSQ159N", "Commercial Real Estate Prices"),
    ("HPIPONM226S", "House Price Index (Purchase only)"),
    ("SP500", "s&p 500"),
    ("NASDAQCOM", "NASDAQ Composite Index"),
    ("DJCA", "Dow Jones Composite Average")
]

def get_data(series_id, series_name, api_key):
    """
    FRED API를 사용하여 특정 시리즈의 데이터를 가져오고 처리하는 함수

    :param series_id: FRED API 시리즈 ID
    :param series_name: 시리즈의 이름 (저장 시 사용)
    :param api_key: FRED API 키
    :return: 처리된 데이터프레임
    """
    url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={api_key}&file_type=json"

    try:
        response = requests.get(url)
        response.raise_for_status()  # HTTP 에러 발생 시 예외 발생
        data = response.json()

        df = pd.DataFrame(data["observations"]).rename(columns={"value": series_name}).set_index("date")

        # 숫자가 아닌 값들의 행 삭제
        df = df[pd.to_numeric(df[series_name], errors='coerce').notna()]

        # realtime* 형식인 컬럼들 삭제
        df = df.loc[:, ~df.columns.str.startswith('realtime')]

        return df

    except requests.RequestException as e:
        logging.error(f"API 요청 중 오류 발생 ({series_name}): {e}")
        return pd.DataFrame()

def save_data(df, filename):
    """
    데이터프레임을 CSV 파일로 저장하는 함수

    :param df: 저장할 데이터프레임
    :param filename: 저장할 파일 이름
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    file_path = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(file_path)
    logging.info(f"데이터가 {file_path}에 저장되었습니다.")

def main():
    """
    메인 실행 함수
    """
    dfs = []
    successful_series = []  # 성공적으로 처리된 시리즈 목록

    # ThreadPoolExecutor를 사용한 병렬 처리
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_series = {executor.submit(get_data, series_id, series_name, API_KEY): (series_id, series_name) for series_id, series_name in SERIES_IDS}

        for future in as_completed(future_to_series):
            series_id, series_name = future_to_series[future]
            try:
                df = future.result()
                if not df.empty:
                    dfs.append(df)
                    successful_series.append(series_name)  # 성공적으로 처리된 시리즈 추가
                    logging.info(f"{series_name} 데이터 처리 완료")
            except Exception as e:
                logging.error(f"{series_name} 처리 중 오류 발생: {e}")

    # 데이터프레임 병합 및 처리
    merged_df = pd.concat(dfs, axis=1, join='outer')
    merged_df.index = pd.to_datetime(merged_df.index)
    merged_df = merged_df.loc[:, ~merged_df.columns.str.startswith('realtime')]
    merged_df.sort_index(inplace=True)

    # 컬럼 순서 변경
    columns_order = [col for _, col in SERIES_IDS if col in merged_df.columns]
    merged_df = merged_df[columns_order]

    # 병합된 데이터 저장
    save_data(merged_df, "economic_indicators.csv")

    # 개별 CSV 파일 삭제 (성공적으로 처리된 시리즈만)
    for series_name in successful_series:
        file_path = os.path.join(OUTPUT_DIR, f"{series_name}.csv")
        if os.path.exists(file_path):
            os.remove(file_path)
            logging.info(f"{file_path} 파일이 삭제되었습니다.")

if __name__ == "__main__":
    main()