# import libraries
import pandas as pd
from sqlalchemy import create_engine
import sqlite3
import sys


def load_data(messages_filepath, categories_filepath):
    messages = pd.read_csv(messages_filepath)
    categories = pd.read_csv(categories_filepath)
    df = messages.merge(categories, on=['id'])
    categories = df['categories'].str.split(";", expand=True)
    row = categories.head(1)
    category_colnames = row.applymap(lambda x: x[:-2]).iloc[0, :].tolist()

    categories.columns = category_colnames
    
    for column in categories:
        categories[column] = categories[column].astype(str).str[-1]
        categories[column] = categories[column].astype(int)
    
    # drop the original categories column from `df`
    df.drop('categories', axis=1, inplace=True)

    # concatenate the original dataframe with the new `categories` dataframe
    df = pd.concat([df, categories], axis=1)
    
    return df

def clean_data(df):
    df.drop_duplicates(inplace=True)
    return df


def save_data(df, database_filepath):
    engine = create_engine('sqlite:///'+ str (database_filepath))
    df.to_sql('Response', engine, index=False, if_exists = 'replace')

def main():
    if len(sys.argv) == 4:

        messages_filepath, categories_filepath, database_filepath = sys.argv[1:]

        print('Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}'
              .format(messages_filepath, categories_filepath))
        df = load_data(messages_filepath, categories_filepath)

        print('Cleaning data...')
        df = clean_data(df)
        
        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        save_data(df, database_filepath)
        
        print('Cleaned data saved to database!')
    
    else:
        print('Please provide the filepaths of the messages and categories '\
              'datasets as the first and second argument respectively, as '\
              'well as the filepath of the database to save the cleaned data '\
              'to as the third argument. \n\nExample: python process_data.py '\
              'disaster_messages.csv disaster_categories.csv '\
              'DisasterResponse.db')


if __name__ == '__main__':
    main()