import pandas as pd
import requests
from dagster import (
    AssetExecutionContext,
    ConfigurableResource,
    Definitions,
    EnvVar,
    asset,
    definitions,
)
from requests.adapters import HTTPAdapter
from sqlalchemy import create_engine
from urllib3.util.retry import Retry


class PostgresResource(ConfigurableResource):
    host: str
    port: int
    dbname: str
    user: str
    password: str

    def get_engine(self):
        url = (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.dbname}"
        )
        return create_engine(url)


def get_http_session() -> requests.Session:
    """Создает HTTP-сессию с повторными попытками и отключенным системным прокси."""
    session = requests.Session()
    session.trust_env = False  # Игнорируем системные прокси Windows
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    })
    return session


@asset(
    key=["external_api", "raw_currency_rates"],
    group_name="ingestion",
    compute_kind="python",
)
def raw_currency_rates(context: AssetExecutionContext, postgres: PostgresResource):
    url = "https://cbr-xml-daily.ru/daily_json.js"
    context.log.info(f"Запрашиваем актуальный курс USD/RUB из ЦБ РФ: {url}")

    session = get_http_session()
    try:
        response = session.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        current_usd_rate = float(data["Valute"]["USD"]["Value"])
        context.log.info(f"Получен живой курс ЦБ РФ (USD/RUB): {current_usd_rate}")
    except (requests.RequestException, ValueError, KeyError) as err:
        context.log.error(f"Ошибка при получении курса валют: {err}")
        raise

    # Генерируем диапазон дат под демо-базу авиаперевозок (июль-сентябрь 2017)
    dates = pd.date_range(start="2017-07-01", end="2017-09-30")
    df = pd.DataFrame({
        "rate_date": dates.date,
        "usd_rate": current_usd_rate,
    })

    with postgres.get_engine().begin() as conn:
        df.to_sql(
            name="raw_currency_rates",
            schema="public",
            con=conn,
            if_exists="replace",
            index=False,
        )
    context.log.info(f"Успешно загружено {len(df)} строк в public.raw_currency_rates")


@definitions
def ingestion_defs():
    return Definitions(
        assets=[raw_currency_rates],
        resources={
            "postgres": PostgresResource(
                host=EnvVar("DB_HOST"),
                port=EnvVar.int("DB_PORT"),
                dbname=EnvVar("DB_NAME"),
                user=EnvVar("DB_USER"),
                password=EnvVar("DB_PASSWORD"),
            )
        },
    )
