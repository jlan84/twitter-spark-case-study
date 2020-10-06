import zipfile

with zipfile.ZipFile('zippedData.zip', 'r') as zip_ref:
    zip_ref.extractall('data/')