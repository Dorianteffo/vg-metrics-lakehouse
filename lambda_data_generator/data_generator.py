import pandas as pd
from faker import Faker
import datetime
import boto3
import random
import io



def generate_data(fake : Faker
                  ) -> pd.DataFrame : 
    
    publishers = [
        'Electronic Arts', 'Activision', 'Ubisoft', 'Nintendo', 'Sony Interactive Entertainment', 
        'Microsoft Studios', 'Take-Two Interactive', 'Rockstar Games', 'Square Enix', 'Capcom', 
        'Sega', 'Bethesda Softworks', 'Bandai Namco Entertainment', 'Konami', '2K Games'
    ]

    # Generate dataset
    data = []

    for _ in range(10000):
        data.append({
            'Name': fake.name(),
            'Year': fake.year(),
            'Platform': fake.random_element(elements=('PC', 'PlayStation', 'Xbox', 'Nintendo Switch', 'Mobile', 'VR', 'Arcade', 'Mac')),
            'Genre': fake.random_element(elements=('action', 'adventure', 'role-playing', 'sports', 'strategy', 'simulation', 'puzzle', 'horror')),
            'Publisher': random.choice(publishers),
            'Rating': fake.random_element(elements=('E', 'T', 'M')),
            'Sales_NA': fake.random_number(digits=6),
            'Sales_EU': fake.random_number(digits=6),
            'Sales_JP': fake.random_number(digits=6),
            'Sales_Other': fake.random_number(digits=6),
            'Total_Sales': fake.random_number(digits=7),
            'Critics_Score': fake.random_number(digits=2, fixed_width=True),
            'User_Score': fake.random_number(digits=2, fixed_width=True),
            'Metacritic_Score': fake.random_number(digits=2, fixed_width=True),
            'IGN_Score': fake.random_number(digits=2, fixed_width=True),
            'ESRB_Rating': fake.random_element(elements=('E for Everyone', 'T for Teen', 'M for Mature')),
            'Multiplayer': fake.random_element(elements=('Yes', 'No')),
            'Downloadable_Content': fake.random_element(elements=('Yes', 'No')),
            'Microtransactions': fake.random_element(elements=('Yes', 'No')),
            'Game_Length': fake.random_number(digits=2, fixed_width=True),
            'Main_Story_Length': fake.random_number(digits=2, fixed_width=True),
            'Completionist_Length': fake.random_number(digits=2, fixed_width=True),
            'Achievements': fake.random_number(digits=2, fixed_width=True),
            'Release_Date': fake.date_time_this_century(before_now=True, after_now=False, tzinfo=None),
            'Game_Mode': fake.random_element(elements=('single-player', 'multiplayer', 'cooperative')),
            'Language_Support': fake.random_element(elements=('English', 'French', 'Spanish', 'German')),
            'Download_Size': fake.random_number(digits=4, fixed_width=True),
            'Community_Rating': fake.random_number(digits=2, fixed_width=True),
            'Player_Count': fake.random_number(digits=5, fixed_width=True),
            'Twitch_Views': fake.random_number(digits=6),
            'YouTube_Views': fake.random_number(digits=6)
        })

    return data



def lambda_handler(event, context):
    fake = Faker()
    df = generate_data(fake)

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    bucket_name = 'vg-sales-raw-data'
    file_name = f"video_games_dataset_{datetime.datetime.now().strftime('%Y-%m-%d_%H')}.csv"
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())