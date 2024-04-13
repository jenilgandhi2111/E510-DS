import sys

word_dict = {}
for word in sys.stdin.read().split(" "):
    word_dict[word] = 1

for word in word_dict.keys():
    print(word + "\tD4")
