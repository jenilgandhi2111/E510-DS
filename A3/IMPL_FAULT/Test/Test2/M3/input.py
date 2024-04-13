import sys
import time
word_dict = {}
for word in sys.stdin.read().split(" "):
    # time.sleep(0.01)
    word_dict[word] = 1

for word in word_dict.keys():
    print(word + "\tD3")
