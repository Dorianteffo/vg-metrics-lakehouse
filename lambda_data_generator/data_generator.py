from faker import Faker
import boto3
import random
import json
from datetime import datetime, timedelta


def generate_game_info(n: int, fake: Faker) -> list:

    publishers = [
        'Electronic Arts',
        'Activision',
        'Ubisoft',
        'Nintendo',
        'Sony Interactive Entertainment',
        'Microsoft Studios',
        'Take-Two Interactive',
        'Rockstar Games',
        'Square Enix',
        'Capcom',
        'Sega',
        'Bethesda Softworks',
        'Bandai Namco Entertainment',
        'Konami',
        '2K Games',
    ]

    game_info = []
    for _ in range(n):
        game = {
            "GameID": fake.uuid4(),
            'Publisher': random.choice(publishers),
            'Rating': fake.random_element(elements=('E', 'T', 'M')),
            "Genre": random.choice(['MMO', 'FPS', 'RPG', 'Adventure', 'Strategy']),
            'Game_Length': fake.random_number(digits=2),
            "ReleaseDate": fake.date_between(
                start_date='-10y', end_date='today'
            ).isoformat(),
        }
        game_info.append(game)

    return game_info


def generate_player_activity(n: int, game_ids: list, fake: Faker):
    player_activity = []
    for _ in range(n):
        start_time = fake.date_time_this_month()
        end_time = start_time + timedelta(minutes=random.randint(30, 300))
        activity = {
            "PlayerID": fake.uuid4(),
            "GameID": random.choice(game_ids),
            "SessionID": fake.uuid4(),
            "StartTime": start_time.isoformat(),
            "EndTime": end_time.isoformat(),
            "ActivityType": random.choice(['Playing', 'AFK', 'In-Queue']),
            "Level": random.randint(1, 100),
            "ExperiencePoints": float(random.randint(100, 10000)),
            "AchievementsUnlocked": random.randint(0, 10),
            "CurrencyEarned": float(random.randint(100, 5000)),
            "CurrencySpent": float(random.randint(0, 3000)),
            "QuestsCompleted": random.randint(0, 20),
            "EnemiesDefeated": random.randint(0, 50),
            "ItemsCollected": random.randint(0, 100),
            "Deaths": random.randint(0, 10),
            "DistanceTraveled": float(random.randint(1, 10000)),
            "ChatMessagesSent": random.randint(0, 100),
            "TeamEventsParticipated": random.randint(0, 5),
            "SkillLevelUp": random.randint(0, 10),
            "PlayMode": random.choice(['Solo', 'Co-op', 'PvP']),
        }
        player_activity.append(activity)

    return player_activity


def upload_to_s3(bucket_name: str, key: str, data: list):
    data_string = json.dumps(data, indent=2)

    # Upload JSON String to an S3 Object
    s3 = boto3.client('s3')

    s3.put_object(Bucket=bucket_name, Key=key, Body=data_string)


def lambda_handler(event, context):
    num_games = 10
    num_players = 10000

    fake = Faker()

    games = generate_game_info(num_games, fake)
    players = generate_player_activity(
        num_players, [game['GameID'] for game in games], fake
    )

    bucket_name = "vg-raw-data"
    games_key = f"Games/{datetime.now().strftime('%Y-%m-%d')}/games_info_{datetime.now().strftime('%Y-%m-%d_%H')}.json"
    players_key = f"Players/{datetime.now().strftime('%Y-%m-%d')}/players_activity_{datetime.now().strftime('%Y-%m-%d_%H')}.json"

    upload_to_s3(bucket_name, games_key, games)
    upload_to_s3(bucket_name, players_key, players)
