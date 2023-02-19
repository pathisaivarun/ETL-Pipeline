import glob
import pandas as pd

def clean_data():
    print("Task started")
    for file in glob.glob("./movies_data/*.csv"):
        print("File found: {}".format(file))
        df = pd.read_csv(file, encoding='latin-1')
        filename = file.split("/")[-1]
        print(filename)
        df["rating"] = pd.to_numeric(df['rating'], errors='coerce')
        df.drop(df[df['year'].isin(['None'])].index, inplace = True)
        df = df[df['year'].str.contains('\d{4}')]
        df["year"] = df['year'].apply(lambda x: x.replace("(", "").replace(")", ""))
        df['year'] = df['year'].str.replace('\D+', '').apply(lambda x: x[-4:])
        df['year'] = df['year'].astype("int")
        df['runtime'] = df["runtime"].apply(lambda x: x.replace("min", "").replace(",", ""))
        df['runtime'] = df['runtime'].astype("int")
        df.drop(df[df['runtime'].apply(lambda x: x <= 0) == True].index, inplace = True)
        df['genre'] = df['genre'].apply(lambda x: x.strip())
        df['rating'] = df['rating'].astype("float")
        df['votes'] = df['votes'].apply(lambda x: x.replace(",", ""))
        df = df[df['votes'].str.contains('^\d+$')]
        df['votes'] = df['votes'].astype("int")
        df.drop('director', axis = 1, inplace=True)
        df.to_csv("./cleaned_movies_data/{}".format(filename), index=False)
    
    print("Data cleaning is done")


def merge_data():
    full_df = None
    for file in glob.glob("./cleaned_movies_data/*.csv"):
        print(file)
        df = pd.read_csv(file)
        if full_df is None:
            full_df = df
        else:
            full_df = pd.concat([full_df, df])
        
    full_df.to_csv("./cleaned_movies_data/all_movies_data.csv", index=False, sep="|")
    print("Successfully created single csv file for all the data")
    #ti.xcom_push(key='data_type', value="movies")