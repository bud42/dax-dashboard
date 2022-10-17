import logging

API_URL = 'https://redcap.vanderbilt.edu/api/'
KEYFILE = os.path.join(os.path.expanduser('~'), '.redcap.txt')

# load reports from redcap and return 
# load latest of each type or all of specific type, or by date or all available

def load_reports(refresh=refresh):
	report_data = []

	if refresh:
		# connect to redcap

		# get list of reports

		# save list

		# download latest PDF for each project for each tyoe of report?

	    try:
	        logging.info('connecting to redcap')
	        i = utils.get_projectid("main", KEYFILE)
	        k = utils.get_projectkey(i, KEYFILE)
	        mainrc = redcap.Project(API_URL, k)

	        double_data = mainrc.export_records(forms=['main', 'double'])

	        print(double_data)

	        # get double entry reports


	        #
	    except Exception as err:
	        logging.error(f'failed to connect to main redcap:{err}')
	        return

