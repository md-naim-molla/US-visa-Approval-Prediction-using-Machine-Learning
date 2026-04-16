from US_Visa_Approval.logger import logging
from US_Visa_Approval.exception import USvisaException
import sys



try:
    r = 3/0
    print(r)

except Exception as e:
    raise USvisaException(e, sys)
