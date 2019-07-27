import sys
import pandas as pd 
from sqlalchemy import create_engine


def load_data(messages_filepath, categories_filepath):
    """
    Load Data function, takes in two csv files and merge them into a master dataframe.
    
    args:
        messages_filepath: path to messages csv file
        categories_filepath: path to categories csv file
    Output:
        df: Loaded dasa as Pandas DataFrame
    """
    # Load messages dataset 
    messages = pd.read_csv(messages_filepath)
    # Load categories dataset 
    categories = pd.read_csv(categories_filepath)
    # Merge both datasets into a master dataset on id column 
    df = pd.merge([messages, categories], how='inner', on='id')
    return df 


def clean_data(df):
    """
    Clean Data function, takes in a dataframe clean it and return that dataframe 
    
    args:
        df: raw data Pandas DataFrame
    Outputs:
        df: clean data Pandas DataFrame
    """
    # Create a dataframe of the 36 individual category columns
    categories = df.categories.str.split(pat=';',expand=True)

    # Select the first row of the categories dataframe
    firstrow = categories.iloc[0,:]

    # Use this row to extract a list of new column names for categories
    category_colnames = firstrow.apply(lambda x:x[:-2])

    # Rename the columns of `categories`
    categories.columns = category_colnames

    for column in categories:
        categories[column] = categories[column].str[-1]  # Set each value to be the last character of the string
        categories[column] = categories[column].astype(int)  # Convert column from string to numeric

    # Drop the original categories column from `df`    
    df = df.drop('categories',axis=1)

    # Concatenate the original dataframe with the new `categories` dataframe
    df = pd.concat([df,categories],axis=1)

    # Drop duplicates
    df = df.drop_duplicates()

    # Return cleaned dataframe 
    return df


def save_data(df, database_filename):
    """
    Save Data function
    
    Arguments:
        df: Clean data Pandas DataFrame
        database_filename: database file (.db) destination path
    """
    engine = create_engine('sqlite:///'+ database_filename)
    df.to_sql('df', engine, index=False)  


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