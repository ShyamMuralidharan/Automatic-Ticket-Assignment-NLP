import re
import pandas as pd
from tqdm import tqdm
import numpy as np


def _clean_emails(df):
    if (len(re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z]+", df["Description"][i]))) > 0:
        df.loc[i, "emails"] = re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z]+", df["Description"][i])[0]
    # df["emails"][i] = re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z]+", df["Description"][i])

    # Removing the Email ID from the short description ------------------------------------
    fltr_txt = ' '.join([item for item in short_desc.split() if '@' not in item])
    return fltr_txt


class Data:
    def __init__(self):
        pass

    def load_data(self, path):
        pass

    def process(self, df, email=True):
        df["emails"] = ""
        df["email_type"] = ""
        df["short_desc_spl"] = ""
        df["description_spl"] = ""
        df["short_desc_len"] = ""
        df["description_len"] = ""
        df["no_of_splch_removed1"] = ""
        df["no_of_splch_removed1_pct"] = ""
        df["no_of_splch_removed2"] = ""
        df["no_of_splch_removed2_pct"] = ""
        df["short_desc_en_spl"] = ""
        df["description_en_spl"] = ""
        df["short_desc_en_len"] = ""
        df["description_en_len"] = ""
        df["trimmed_words"] = ""
        df["trimmed_words_len"] = ""
        df["trimmed_words_short"] = ""
        df["trimmed_words_long"] = ""
        df["mark_for_delete"] = 0

        x = 0
        if email:
            df['email_type']=_clean_emails(df)

        for i in tqdm(range(len(df))):
            short_desc = str(df["Short description"][i]).encode('ascii', 'ignore').decode()
            df.loc[i, "no_of_splch_removed1"] = len(df["Short description"][i]) - len(
                str(df["Short description"][i]).encode('ascii', 'ignore').decode())
            df.loc[i, "no_of_splch_removed1_pct"] = np.round(
                (df.loc[i, "no_of_splch_removed1"] * 100 / len(df["Short description"][i])), 1)
            desc = str(df["Description"][i]).encode('ascii', 'ignore').decode()
            df.loc[i, "no_of_splch_removed2"] = len(df["Description"][i]) - len(
                str(df["Description"][i]).encode('ascii', 'ignore').decode())
            df.loc[i, "no_of_splch_removed2_pct"] = np.round(
                (df.loc[i, "no_of_splch_removed2"] * 100 / len(df["Description"][i])), 1)

            # The below Funtion is to transilate the sentances to English but this free API has miliation. Hence not used.
            # We used the pre transilated input data (using Googl sheet)
            # if b == "en" :
            #      trn = Translator().translate(short_desc, dest = "en", src = "auto")
            #      short_desc_en = trn.text
            #      trn1 = Translator().translate(desc, dest = "en", src = "auto")
            #      Description_en = trn1.text
            # else :

            # The below function is to remove Junk Characters from text
            short_desc_en = str(df["short_desc_en"][i]).encode('ascii', 'ignore').decode()
            Description_en = str(df["description_en"][i]).encode('ascii', 'ignore').decode()

            # Finding the first Email Address from the 'Description' column.
            if (len(re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z]+", df["Description"][i]))) > 0:
                df.loc[i, "emails"] = re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z]+", df["Description"][i])[0]
            # df["emails"][i] = re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z]+", df["Description"][i])

            # Removing the Email ID from the short description ------------------------------------
            fltr_txt = ' '.join([item for item in short_desc.split() if '@' not in item])
            fltr_txt = re.sub(r"received from:", "", fltr_txt)
            alpha = ""
            for char in fltr_txt:
                if char.isspace():
                    alpha += " "
                if char.isalnum():
                    alpha += char.lower()
            df.loc[i, "short_desc_spl"] = str(" ".join(alpha.split()))
            df.loc[i, "short_desc_len"] = len(alpha.split())
            # df.loc[i,"no_splch_removed"] = len(fltr_txt)-len(alpha.split())
            # Removing the Email ID from the Description-------------------------------------------------
            fltr_txt1 = ' '.join([item for item in desc.split() if '@' not in item])
            fltr_txt1 = re.sub(r"received from:", "", fltr_txt1)
            alpha1 = ""
            for char in fltr_txt1:
                if char.isspace():
                    alpha1 += " "
                if char.isalnum():
                    alpha1 += char.lower()
            df.loc[i, "description_spl"] = str(" ".join(alpha1.split()))
            df.loc[i, "description_len"] = len(alpha1.split())
            # ----------------------------
            fltr_txt = ' '.join([item for item in short_desc_en.split() if '@' not in item])
            fltr_txt = re.sub(r"received from:", "", fltr_txt)
            alpha = ""
            for char in fltr_txt:
                if char.isspace():
                    alpha += " "
                if char.isalnum():
                    alpha += char.lower()
            df.loc[i, "short_desc_en_spl"] = str(" ".join(alpha.split()))
            df.loc[i, "short_desc_en_len"] = len(alpha.split())
            # ---------------------------------
            fltr_txt1 = ' '.join([item for item in Description_en.split() if '@' not in item])
            fltr_txt1 = re.sub(r"received from:", "", fltr_txt1)
            alpha1 = ""
            for char in fltr_txt1:
                if char.isspace():
                    alpha1 += " "
                if char.isalnum():
                    alpha1 += char.lower()
            df.loc[i, "description_en_spl"] = str(" ".join(alpha1.split()))
            df.loc[i, "description_en_len"] = len(alpha1.split())

            # --------------------------------------
            # Final Text after stop word removal and combining both short and long description

            df.loc[i, "trimmed_words_short"] = " ".join(
                [item for item in df["short_desc_en_spl"][i].split() if item not in stop_words1])
            df.loc[i, "trimmed_words_long"] = " ".join(
                [item for item in df["description_en_spl"][i].split() if item not in stop_words1])

            if df["short_desc_en_spl"][i] == df["description_en_spl"][i]:
                df.loc[i, "trimmed_words"] = " ".join(
                    [item for item in df["short_desc_en_spl"][i].split() if item not in stop_words1])
            else:
                df.loc[i, "trimmed_words"] = " ".join(
                    [item for item in df["short_desc_en_spl"][i].split() if item not in stop_words1]) + " " + " ".join(
                    [item for item in df["description_en_spl"][i].split() if item not in stop_words1])
            # Mark records with lot of junk characters for  delete -----------------------------------------------
            df.loc[i, "trimmed_words_len"] = len(df["trimmed_words"][i].split())
            if df["trimmed_words_len"][i] <= 1:
                df.loc[i, "mark_for_delete"] = 1
            if df["no_of_splch_removed1_pct"][i] > 50:  # sentances with more than 50% junk
                df.loc[i, "mark_for_delete"] = 1
            if i in short_null:
                df.loc[i, "mark_for_delete"] = 1

