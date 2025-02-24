from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()
def get_db_connection():
    """
    .env 파일에 저장된 DB 정보를 사용하여 SQLAlchemy 엔진을 생성.
    """
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    if not all([db_user, db_password, db_host, db_port, db_name]):
        raise ValueError("DB 환경변수가 올바르게 설정되지 않았습니다.")

    return create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

def get_db_connection():
    """
    .env 파일에 저장된 DB 정보를 사용하여 SQLAlchemy 엔진을 생성.
    """
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    if not all([db_user, db_password, db_host, db_port, db_name]):
        raise ValueError("DB 환경변수가 올바르게 설정되지 않았습니다.")

    return create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")


def upload_dataframe_to_mysql(df, table_name, if_exists="append", chunksize=1000):
    """
    데이터프레임을 MySQL 테이블에 업로드.

    Args:
        df (pd.DataFrame): 업로드할 데이터프레임.
        table_name (str): 업로드할 테이블 이름.
        if_exists (str): 테이블이 존재할 경우 동작. {"fail", "replace", "append"} (기본값: "append").
        chunksize (int): 데이터를 한번에 업로드할 행 수 (기본값: 1000).

    Raises:
        ValueError: 데이터프레임이 비었거나 DB 연결 실패 시.
    """
    if df.empty:
        raise ValueError("업로드할 데이터프레임이 비어 있습니다.")

    try:
        engine = get_db_connection()
        with engine.begin() as connection:
            df.to_sql(
                name=table_name,
                con=connection,
                if_exists=if_exists,
                index=False,
                chunksize=chunksize,
                method="multi"
            )
        print(f"테이블 '{table_name}'에 데이터 업로드 완료!")
    except Exception as e:
        print(f"데이터 업로드 중 오류 발생: {e}")


def read_table_to_dataframe(table_name):
    """
    MySQL 테이블 데이터를 Pandas DataFrame으로 읽어옴.

    Args:
        table_name (str): 읽어올 테이블 이름.

    Returns:
        pd.DataFrame: 테이블 데이터가 포함된 DataFrame.
    """
    try:
        engine = get_db_connection()
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, con=engine)
        print(f"테이블 '{table_name}' 데이터가 성공적으로 DataFrame으로 로드되었습니다.")
        return df
    except Exception as e:
        print(f"오류 발생: {e}")
        return None

def execute_query(query, fetch=False):
    """
    SQL 쿼리를 실행하는 함수.

    Args:
        query (str): 실행할 SQL 쿼리.
        fetch (bool): 데이터를 반환할지 여부. True이면 데이터를 반환.

    Returns:
        list 또는 None: fetch=True일 때 결과를 리스트로 반환, fetch=False일 경우 None.
    """
    try:
        engine = get_db_connection()
        with engine.connect() as connection:
            if fetch:
                result = connection.execute(query)
                return [row for row in result]  # 결과를 리스트로 변환
            else:
                connection.execute(query)  # 데이터 수정/삽입/삭제
    except SQLAlchemyError as e:
        print(f"쿼리 실행 중 오류 발생: {e}")
        return None