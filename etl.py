from zipfile import ZipFile
import pandas as pd
import os
import zipfile
import shutil


def zip_dir(path, filename, ext):
    tmpzip = zipfile.ZipFile(
        f'{path}/{filename}.zip', mode="w")
    tmpzip.write(f'./{path}/{filename}.{ext}', f'{filename}.{ext}')
    tmpzip.close()


def process():
    path = 'output'
    sourceZip = ZipFile('data/cur.zip')
    f = sourceZip.open('output.csv')
    df_csv = pd.read_csv(f, low_memory=False)
    df_json = pd.read_json('data/fix.json')

    df_csv = df_csv.loc[(df_csv['product/ProductName'] == 'Amazon CloudFront')
                        & (df_csv['lineItem/LineItemType'] == 'Usage')]

    df_csv = df_csv.set_index(
        ['lineItem/UsageAccountId', 'lineItem/UsageType'])
    dfjson = df_json.set_index(
        ['lineItem/UsageAccountId', 'lineItem/UsageType'])

    df_csv.update(dfjson)
    df_csv.reset_index()

    df_csv['lineItem/UnblendedCost'] = df_csv['lineItem/UsageAmount'] * \
        df_csv['lineItem/UnblendedRate']

    if not os.path.isdir(path):
        os.mkdir(path)
    else:
        shutil.rmtree(path)
        os.mkdir(path)

    for gender, group in df_csv.groupby(['lineItem/UsageAccountId']):
        intg = int(gender)
        group.to_csv(f'{path}/{intg}.csv', index=False)
        zip_dir(path, intg, 'csv')
        os.remove(f'{path}/{intg}.csv')
        print(group)

    f.close()
    sourceZip.close()


if __name__ == "__main__":
    process()
