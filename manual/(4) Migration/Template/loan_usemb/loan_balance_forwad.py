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
		
    logger.info("start update")

#Update 
    for tid in res['ids']:
	
	#update loan header 
        logger.info("update loan header start.id:" + str(tid))
        loan_header = models.execute_kw(conn.db, conn.uid, conn.password, 'wc.loan', 'search_read',
            [[['id','=',tid]]],
            {'fields':['state','date','amount','note']})
        models.execute_kw(conn.db, conn.uid, conn.password, 'wc.loan', 'write', 
            [tid,
            {'state':'approved',
             'maturity_period':'weeks',
             'days_in_year':'364',
             'is_interest_deduction_first':1,
             'interest_rate':6.5,
             'is_interest_epr':1,
             'is_collect_penalty':0,
             'payment_schedule':'x-days',
             'payment_schedule_xdays':14,
             'date_application':loan_header[0]['date'],
             'date_start':loan_header[0]['date']}])

        #update date_maturity by using temporarily saved field(note: needs to be updated seperately because date_maturity is compute field)
        models.execute_kw(conn.db, conn.uid, conn.password, 'wc.loan', 'write', 
            [tid,
            {'date_maturity':loan_header[0]['note'],
             'note':'this loan is migrated data as balance forward loan'}])



        #update loan detail
        logger.info("update loan detail start.")

	loan_detail_data = models.execute_kw(conn.db, conn.uid, conn.password, 'wc.loan.detail', 'search_read',
            [[['loan_id','=',tid]]],
            {'fields':['principal_due','interest_due','principal_paid','interest_paid','id','date_due']})

        models.execute_kw(conn.db, conn.uid, conn.password, 'wc.loan.detail', 'write', 
            [loan_detail_data[0]['id'],
            {'sequence':0,
             'date_start':loan_header[0]['date'],
             'date_due':loan_header[0]['date'],
             'principal_paid':loan_detail_data[0]['principal_due'],
             'interest_paid':loan_detail_data[0]['interest_due'],
             'state':'paid'}])

        #add dummy payment record from loan detail
             #principal
        logger.info("create payment distribution PHP start")
        models.execute_kw(conn.db, conn.uid, conn.password, 'wc.loan.payment.distribution', 'create', 
            [{'detail_id':loan_detail_data[0]['id'],
             'state':'confirmed',
             'date':loan_header[0]['date'],
             'code':'PCP',
             'amount':loan_detail_data[0]['principal_due'],
             'payment_type':'principal',
             'state':'confirmed'}])
            #interest
        logger.info("create payment distribution INT start")
        models.execute_kw(conn.db, conn.uid, conn.password, 'wc.loan.payment.distribution', 'create', 
            [{'detail_id':loan_detail_data[0]['id'],
             'state':'confirmed',
             'date':loan_header[0]['date'],
             'code':'INT',
             'amount':loan_detail_data[0]['interest_due'],
             'payment_type':'interest',
             'state':'confirmed'}])

        #make next_due record into loan detail from amortization
            #get amortization record
        loan_amort_data = models.execute_kw(conn.db, conn.uid, conn.password, 'wc.loan.amortization', 'search_read',
            [[['loan_id','=',tid]]],
            {'fields':['id','date_due','date_start','sequence','principal_balance','principal_due','interest_due','days','state']})

        loan_amort_data_sorted=sorted(loan_amort_data,key=lambda r: r['sequence'])
        loan_amort_first_data = loan_amort_data_sorted[0]
        
            #add loan detail
        models.execute_kw(conn.db, conn.uid, conn.password, 'wc.loan.detail', 'create', 
            [{'loan_id':tid,
             'sequence':loan_amort_first_data['sequence'],
             'date_due':loan_amort_first_data['date_due'],
             'date_start':loan_amort_first_data['date_start'],
             'principal_due':loan_amort_first_data['principal_due'],
             'interest_due':loan_amort_first_data['interest_due'],
             'state':'next_due'}])
            #update amortization record's status
        models.execute_kw(conn.db, conn.uid, conn.password, 'wc.loan.amortization', 'write', 
            [loan_amort_first_data['id'],{'state':'processed'}])

		
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
    
    logger.info("start import")
	
    #********modify here if loan header files is divided several files*************
    load_file(conn, "wc.loan", "loan_header_and_amortization.csv", logger)
    #******************************************************************************	
    
  
    
    #***************update database table******************

