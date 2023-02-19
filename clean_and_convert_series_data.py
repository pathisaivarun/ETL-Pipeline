import glob
import pandas as pd

def clean_data():
    print("Task started")
    for file in glob.glob("./series_data/*.csv"):
        print("File found: {}".format(file))
        df = pd.read_csv(file, encoding='latin-1')
        filename = file.split("/")[-1]
        print(filename)
        df.drop('director',axis=1, inplace=True)
        df.drop(df[df['year'].isin(['None'])].index, inplace = True)
        df['year'] = df['year'].apply(lambda x: x.replace("(", "").replace(")", "").strip())
        df.drop(df[df['year'].str.contains('\d{4,}')==False].index, inplace=True)
        df['start_year'] = df['year'].apply(lambda x: x.split('–')[0])
        df['start_year'] = df['start_year'].str.extract(r'(\d{4})').astype(int)
        df['end_year']= df['year'].apply(lambda x: x.split('–')[-1])
        df['end_year'] = df['end_year'].apply(lambda x: x.replace('', '2022') if x=="" else x)
        df['end_year'] = df['end_year'].str.extract(r'(\d{4})').astype(int)
        df['runtime'] = df["runtime"].apply(lambda x: x.replace("min", ""))
        df['runtime'] = df['runtime'].apply(lambda x: x.replace(',', ''))
        df['runtime'] = df['runtime'].astype('int')
        df['genre'] = df['genre'].apply(lambda x: x.strip())
        df['rating'] = df['rating'].astype('float')
        df['votes'] = df['votes'].apply(lambda x: x.replace(',', ''))
        df.drop(df[df['votes'].str.contains('^\d+$')==False].index, inplace=True)
        df['votes'] = df['votes'].astype('int')
        df.drop('year', axis=1, inplace=True)
        df.to_csv("./cleaned_series_data/{}".format(filename), index=False)
    
    print("Data cleaning is done")


def merge_data(ti):
    full_df = None
    for file in glob.glob("./cleaned_series_data/*.csv"):
        print(file)
        df = pd.read_csv(file)
        if full_df is None:
            full_df = df
        else:
            full_df = pd.concat([full_df, df])
        
    full_df.to_csv("./cleaned_series_data/all_series_data.csv", index=False, sep="|")
    print("Successfully created single csv file for all the data")   
    #ti.xcom_push(key='data_type', value="series")
