# =========================================
# 	Author: Jenil J Gandhi
# 	Subject: Distributed Systems
# 	Assignment Number: 3
# 	Date: Date: 11-03-2024 22:32:30
# =========================================

import sys
from MasterWorker import Master

if len(sys.argv) < 2:
    print("Usage: python master.py <TESTCASENUMBER>")

Master(int(sys.argv[1]))
