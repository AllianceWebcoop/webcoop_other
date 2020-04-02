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
       raise ValueError

    print "res:", res
    if res['ids']>0:
        print filename
        logmessage = "Import successfully. table = %s ,records id = %s " % (table, res['ids'])
        logger.info(logmessage)
    else:
        logger.exception('Data Validation Error:%s' % res)
        raise ValueError
		
    #update interest_rate,penalty_rate,
    #interest_rate, penalty_rate,payment_schedule,days_in_year, is_fixed_payment_amount, is_interest_epr by loan_type master
    #logger.info("start update")


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
    
    #import 
    try :
        logger.info("start import")
        load_file(conn, "wc.loan.amortization", "loan.amortization.csv", logger)
    except Exception ,e:
        logger.exception('Failed')
    #***************update database table******************

