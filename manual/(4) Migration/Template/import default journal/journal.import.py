import xlrd
import xmlrpclib
import csv
import traceback
import logging
import ConfigParser

CHUNK_SIZE = 50
THREADS = 4

class Struct(object):
    def __init__(self, **entries):
        self.__dict__.update(entries)


def read_csv(filename):

    csv_file = open(filename,'r')

    #record count check , throw error if over 1000 
    if len(open(filename).readlines()) > 1001:
    	raise ValueError('Too much record for import, please re-make the file by within 1000 records')
		
    f = csv.reader(csv_file, delimiter=",", doublequote=True, lineterminator="\r\n", quotechar='"', skipinitialspace=True)
	
    i = 0
    data = []
    for row in f:
	if i == 0:
           header = row
           i = 1
        else:
           data.append(row)
    return header, data


def load_file(conn, table, filename, logger):

    #read csv file
    try :
       header, data = read_csv(filename)
    except Exception ,e:
       logger.exception('CSV read Failed')
       raise
    try :
       models = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(conn.url))
       res = models.execute_kw(conn.db, conn.uid, conn.password,
           table, 'load', [header, data]
       );
    except Exception ,e:
       logger.exception('Import Failed')
       raise

    print "res:", res
    if res['ids']>0:
        print filename
        logmessage = "Import successfully. table = %s ,records id = %s " % (table, res['ids'])
        logger.info(logmessage)
    else:
        logger.exception('Data Validation Error:%s' % res)
        raise ValueError
    #print

def delete_all_record(conn, table, logger):
    try :
       models = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(conn.url))
       search_ids = models.execute_kw(conn.db, conn.uid, conn.password, table, 'search', [[]])
       models.execute_kw(conn.db, conn.uid, conn.password, table, 'unlink', [search_ids])
    except Exception ,e:
       logger.exception('Cannot delete current record')
       raise
    logmessage = "delete successfully.table = %s , records id = %s" % (table, [search_ids])
    logger.info(logmessage)

   
	
if __name__=="__main__":
    #log file handele
    logger = logging.getLogger('LoggingTest')
    logger.setLevel(10)
    fh = logging.FileHandler('error.log')
    logger.addHandler(fh)
    sh = logging.StreamHandler()
    logger.addHandler(sh)
    formatter = logging.Formatter('%(asctime)s:%(lineno)d:%(levelname)s:%(message)s')
    fh.setFormatter(formatter)
    sh.setFormatter(formatter)

    #set config file
    config = ConfigParser.ConfigParser()
    config.read('config.ini')
	
    #*************connection info(get from config.ini)**************
	
    url = config.get('connection_info','url')
    admin = config.get('connection_info','admin')
    admin_password = config.get('connection_info','admin_password')
    db = config.get('connection_info','db')
    #*************connection info**************

    try :
       common = xmlrpclib.ServerProxy('{}/xmlrpc/2/common'.format(url))
    except Exception ,e:
       logger.exception('Connection Failed')
       raise

    print "VERSION", common.version()
    try :
       uid = common.authenticate(db, admin, admin_password, {})
    except Exception ,e:
       logger.exception('Authenticate Failed')
       raise

    print "UID:", uid

    p = {
        'uid': uid,
        'password': admin_password,
        'db': db,
        'url': url,
    }
    conn = Struct(**p)
    
    #***************update database table******************
    
    #import journal info
    logger.info("start import account_journal")
    load_file(conn, "account.journal", "journal.import.csv", logger)
    
    #update default_credit_account_id ,default_debit_account_id in journal CR,CV
    models = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(url))   
    update_values = models.execute_kw(conn.db, conn.uid, conn.password, 'account.account', 'search', 
         [[('company_id','=',1),('name','=','Cash')]])
    if len(update_values) == 0:
         raise ValueError('no Cash account data in company 1 , or no main company')

    target_ids = models.execute_kw(conn.db, conn.uid, conn.password, 'account.journal','search',
         [[('company_id','=',1),'|',('code','=','CR'),('code','=','CV')]])
    if len(target_ids) == 0:
         raise ValueError('no CR,CR journal in company 1 , or no main company')
    
    models.execute_kw(conn.db, conn.uid, conn.password, 'account.journal', 'write', 
         [target_ids,
         {'default_credit_account_id':update_values[0],'default_debit_account_id':update_values[0]}])

    
    #***************update database table******************

