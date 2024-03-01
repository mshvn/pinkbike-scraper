import re
from datetime import datetime, timedelta

import pandas as pd
import pendulum
import requests
from airflow import DAG

from airflow.decorators import task
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, min, max, replace, mean, mode


# # # - - - - - - - - - - - - - - - - - - - - - - - - - # # #
# # #                   "DEF" section                   # # #
# # # - - - - - - - - - - - - - - - - - - - - - - - - - # # #


# SCRAPPING NEWS
def scrapper_pinkbike():
    """
    Retrieves data from the Pinkbike.com main page
    """

    pink_req = requests.get("https://www.pinkbike.com")
    soup = BeautifulSoup(pink_req.content, "html.parser")

    allNewstext = soup.findAll("a", class_="f22 fgrey4 bold")
    allNewscomments = soup.findAll("a", class_="fgrey")

    news_list = []
    comments_num_list = []

    for tag in allNewstext:
        if tag.img.get("src"):
            continue
        else:
            news_list.append(
                [tag.text.strip(), tag["href"], tag.img.get("data-src")]
            )

    for news_item in allNewscomments:
        if news_item is not None:
            comments_num_list.append(
                [
                    news_item.get("href")[:-11],
                    int(re.search(r"\d+", news_item.text)[0]),
                ]
            )

    df_news = pd.DataFrame(news_list, columns=["title", "link", "image"])
    df_comments = pd.DataFrame(comments_num_list, columns=["link", "comments"])

    df_out = df_news.join(df_comments.set_index("link"), on="link", how="left")
    df_out["comments"] = df_out["comments"].fillna(0).astype(int)
    df_out = df_out.sort_values(by=["comments"], ascending=False)

    print(df_out.head())
    print("Mean:", df_out["comments"].mean())

    return df_out


# SCRAPPING EVENTS
def scrapper_pinkbike_events():
    """
    Retrieves data from the Pinkbike.com Events page
    """

    pink_req = requests.get("https://www.pinkbike.com/news/racing/")
    soup = BeautifulSoup(pink_req.content, "html.parser")

    allEvents = soup.findAll("a", class_="f22 fgrey4 bold")
    allEventscomments = soup.findAll("a", class_="fgrey")
    allEventsdatas = soup.findAll("span", class_="fgrey2")

    lst_titles = []
    comments_events = []
    data_events = []
    links_events = []

    for i in range(0, len(allEvents)):
        event = (
            str(allEvents[i])
            .split("/>")[-1]
            .replace("</a>", "")
            .replace("\n", "")
            .strip()
        )
        if event[0] == "<":
            pass
        else:
            try:
                comments = int(str(allEventscomments[i]).split(">")[-2].split(" ")[0])
                comments_events.append(comments)
            except IndexError:
                comments_events.append(0)

    for i in range(0, len(allEvents)):
        event = (
            str(allEvents[i])
            .split("/>")[-1]
            .replace("</a>", "")
            .replace("\n", "")
            .strip()
        )
        href = allEvents[i]["href"]
        if event[0] == "<":
            pass
        else:
            links_events.append(href)

    for i in range(0, len(allEvents)):
        event = (
            str(allEvents[i])
            .split("/>")[-1]
            .replace("</a>", "")
            .replace("\n", "")
            .strip()
        )
        if event[0] == "<":
            pass
        else:
            lst_titles.append(event)

    for i in range(0, len(allEventsdatas)):
        event = (
            str(allEvents[i])
            .split("/>")[-1]
            .replace("</a>", "")
            .replace("\n", "")
            .strip()
        )
        if event[0] == "<":
            pass
        else:
            data = str(allEventsdatas[i]).split("\n")[-1].split("</")[0].strip()
            if len(data) > 13:
                data_events.append(data[-5:])
            else:
                data_events.append(data)

    dict = {
        "Title": lst_titles,
        "Links": links_events,
        "Comments": comments_events,
        "Data": data_events,
    }
    df = pd.DataFrame(dict)
    df = df.replace("Today", datetime.now().strftime("'%b %d %Y'")[1:-1])

    dates = []
    for dt in df.Data:
        dates.append(datetime.strptime(dt.replace(",", ""), "%b %d %Y").date())

    df["Data NF"] = dates

    df_sort = df.sort_values(by="Data NF", ascending=False)

    df_sort = df_sort[df_sort["Data NF"] > datetime.today().date() - timedelta(days=30)]
    df_sort = df_sort.sort_values(by="Comments", ascending=False)

    df_out = df_sort.reset_index(drop=True)[:3]

    return df_out


# NEWS TO TG
def news_to_telegram(df_out, BOT_TOKEN, CHANNEL_ID, URL):
    """
    Post news to Telegram channel
    """
    message = "TOP NEWS:\n\n"

    for i in range(0, 5):
        message = (
            message
            + df_out.iloc[i]["title"]
            + "\n"
            + df_out.iloc[i]["link"]
            + "\n"
            + "Comments: "
            + str(df_out.iloc[i]["comments"])
            + "\n\n"
        )

    data = {"chat_id": CHANNEL_ID, "text": message, "parse_mode": "HTML"}
    response = requests.post(URL, data=data)
    print("Response:", response.json())

    return


# EVENTS TO TG
def events_to_telegram(df_out, BOT_TOKEN, CHANNEL_ID, URL):
    """
    Post Events to Telegram channel
    """
    message = "FRESH EVENTS:\n\n"

    for i in range(0, 3):
        message = (
            message
            + "Top "
            + str(i + 1)
            + " event: "
            + df_out.iloc[i]["Title"]
            + "\n"
            + "Link: "
            + df_out.iloc[i]["Links"]
            + "\n"
            + "Date: "
            + df_out.iloc[i]["Data"]
            + "\n"
            + "Comments: "
            + str(df_out.iloc[i]["Comments"])
            + "\n\n"
        )

    data = {"chat_id": CHANNEL_ID, "text": message, "parse_mode": "HTML"}
    response = requests.post(URL, data=data)
    print("Response:", response.json())

    return


# # # - - - - - - - - - - - - - - - - - - - - - - - - - # # #
# # #                   DAG starts here                 # # #
# # # - - - - - - - - - - - - - - - - - - - - - - - - - # # #


default_args = {
    "email": ["example@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id="pinkbike_com_dag_01",
    start_date=pendulum.datetime(2023, 12, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    schedule_interval="0 */4 * * *",
    dagrun_timeout=timedelta(seconds=120),
) as dag:

    @task(task_id="get_pink_data")
    def get_pink_data(**kwargs):
        """
        Scrapping data, posting to Telegram, saving to parquet
        """
        BOT_TOKEN = "YOUR_BOT_TOKEN_HERE"
        CHANNEL_ID = "YOUR_CHANNEL_ID_HERE"
        URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        current_date = kwargs["ds"]

        if (datetime.utcnow().time().hour) % 8 == 0:
            # scrapping Events
            df_out_events = scrapper_pinkbike_events()
            # Events to telegram
            events_to_telegram(df_out_events, BOT_TOKEN, CHANNEL_ID, URL)
        
        # scrapping News
        df_out_news = scrapper_pinkbike()

        # News to telegram
        news_to_telegram(df_out_news, BOT_TOKEN, CHANNEL_ID, URL)

        # News to spark
        spark = (
            SparkSession.builder.master("local[*]")
            .appName("dataframe_pink")
            .getOrCreate()
        )
        df = spark.createDataFrame(df_out_news)
        df.write.mode("overwrite").parquet("/user/user_name/DATA/DT=" + current_date)


    @task(task_id="agg_pink_data")
    def agg_pink_data(**kwargs):
        """
        Aggregate the collected data
        """
        current_date = kwargs["ds"]
        spark = (
            SparkSession.builder.master("local[*]")
            .appName("dataframe_pink_2")
            .getOrCreate()
        )

        # new version
        df = spark.read.parquet("/user/user_name/DATA/DT=" + current_date)
        
        df_agg = df.agg(max(col("comments")).alias("Max"), 
                        min(col("comments")).alias("Min"), 
                        mean(col("comments")).alias("Avg"), 
                        mode(col("comments")).alias("Mode"))

        print("df_agg:")
        print(df_agg.show())

        df_agg.write.mode("overwrite").parquet("/user/user_name/AGG/DT=" + current_date)


    @task(task_id="to_mysql_pink")
    def to_mysql_pink(**kwargs):
        """
        Aggregated data ---> to MySQl
        """
        current_date = kwargs["ds"]

        spark = SparkSession.builder\
                    .master("local[*]")\
                    .appName('dataframe_exercise')\
                    .config("spark.jars", "/usr/share/java/mysql-connector-java-8.2.0.jar")\
                    .getOrCreate()

        # old version
        df = spark.read.parquet("/user/user_name/AGG")

        df\
            .write\
            .mode("overwrite")\
            .format("jdbc")\
            .option("driver","com.mysql.cj.jdbc.Driver")\
            .option("url", "jdbc:mysql://localhost:3306/hse")\
            .option("dbtable", "pinkbike_comments")\
            .option("user", "user_name_001")\
            .option("password", "secret_password")\
            .save()


    get_pink_data() >> agg_pink_data() >> to_mysql_pink()
