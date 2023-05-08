import sqlalchemy
import csv
import datetime

dbEngine = sqlalchemy.create_engine("postgresql+psycopg2://postgres:stdio.h@localhost:5432/test")

####################################################################################################
def insertData(file,path):
    """
    :param file: String | Name of the file with extension
    :param path: String | Path where the file is stored
    :return: returns None.
    """
    
    try:
        with open(f"{path}/{file}", 'r') as csvFile:
            fileName = file.split('.')[0]
            with dbEngine.connect() as con:
                csvReader = csv.reader(csvFile)
                for chunk in createChunks(csvReader):
                    for row in chunk:
                        if '' in row:
                            writeCorruptedData(fileName,row,path)
                        else:
                            sent = setSentence(fileName,row)
                            con.execute(sent)
                            
    except Exception as e:
        print (e, flush = True)
        return None

    return 'All good'

####################################################################################################

def setSentence(fileName,row):
    """
    :param fileName: String | Name with the file without extension
    :param row: Array | List that contains the data of the row
    :return: String | Sentence of the insert expression
    """

    try:

        col = {
            'departments':'id,department',
            'hired_employees':'id,name,datetime,department_id,job_id',
            'jobs':'id,job'
            }
        
        sentence = ''

        if fileName == 'hired_employees':
            sentence = f"""INSERT INTO {fileName} ({col[fileName]}) VALUES
            ({row[0]}, $${row[1]}$$, $${row[2]}$$, {row[3]}, {row[4]});"""
        elif fileName == 'departments':
            sentence = f"""INSERT INTO {fileName} ({col[fileName]}) VALUES
            ({row[0]}, $${row[1]}$$);"""
        elif fileName == 'jobs':
            sentence = f"""INSERT INTO {fileName} ({col[fileName]}) VALUES
            ({row[0]}, $${row[1]}$$);"""

        return sentence
    
    except Exception as e:
        print (e, flush = True)
        return None

####################################################################################################

def writeCorruptedData(fileName,row,path):
    """
    :param fileName: String | Name with the file without extension
    :param row: Array | List that contains the data of the row
    :param path: String | Path where the file is stored
    :return: returns None.
    """
    try:
        with open(f"{path}/corrupted/{fileName}_{datetime.date.today()}.csv", 'a') as corruptedFile:
                writer = csv.writer(corruptedFile)
                writer.writerow(row)
        return 'Data corrupted writed succesfully'
    except Exception as e:
        print (e, flush = True)
        return None

####################################################################################################

def createChunks(reader, chunksize=1000):
    """ 
    :param reader: Object | CSV reader object
    :param chunksize: Int | Number of rows per chunk
    :return: Array | Chunk with rows
    """
    try:
        chunk = []
        for i, line in enumerate(reader):
            if (i % chunksize == 0 and i > 0):
                yield chunk
                del chunk[:]
            chunk.append(line)
        yield chunk
    except Exception as e:
        print (e, flush = True)
        return None