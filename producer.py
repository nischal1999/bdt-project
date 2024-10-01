import requests
from kafka import KafkaProducer
from time import sleep
import json

# Configuration
KAFKA_TOPIC = 'news-articles'
KAFKA_SERVER = 'localhost:9092'
NEWS_API_KEY = '0902821000cd4a7d91e8ffe7a60e8bce'
NEWS_API_URL = f"https://newsapi.org/v2/everything?q=technology&apiKey={NEWS_API_KEY}"

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def fetch_news_articles(url):
    """Fetch news articles from the News API."""
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx, 5xx)
        return response.json().get('articles', [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching news articles: {e}")
        return []


def construct_news_article(article):
    """Construct a news article dictionary from the raw API response."""
    return {
        'title': article.get('title', 'No Title'),
        'description': article.get('description', 'No Description'),
        'content': article.get('content', 'No Content'),
        'published_at': article.get('publishedAt', 'No Date'),
        'source': article.get('source', {}).get('name', 'Unknown Source'),
        'url': article.get('url', 'No URL')
    }


def stream_articles_to_kafka(articles):
    """Stream each article to the Kafka topic."""
    for article in articles:
        news_article = construct_news_article(article)
        producer.send(KAFKA_TOPIC, value=news_article)
        print(f"Sent news article to Kafka: {news_article['title']}")

        # Simulate streaming by sleeping for a short time
        sleep(1)
    producer.flush()


def main():
    articles = fetch_news_articles(NEWS_API_URL)
    if articles:
        stream_articles_to_kafka(articles)
    else:
        print("No articles to stream.")


if __name__ == "__main__":
    main()
