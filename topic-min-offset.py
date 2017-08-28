import time
import glob
import os

# topic = GO_RIDE-booking-log-0

consumer_group = 'sample_consumer_group'
topic = 'a'
date_from = ''

kafka_logs_dir = '/Users/deepakmarathe/workspace/python/'

# convert date to epoch timestamp
timestruct = time.strptime(date_from, "%d-%b-%y")
epoch_time = time.mktime(timestruct)


def getIndexFromFile(file):
    return file.split('/')[-1].split('.')[0]


def get_min_offset_for_date(kafka_logs_dir, topic, e_time):
    partition_dir = glob.glob(kafka_logs_dir + topic + '-*')
    # print("topic : " + topic + ", PARTITIONS : ")
    partition_offset = {}
    for dir in partition_dir:
        offset = getoffset(dir + "/", e_time)
        partition_offset[dir] = offset
        # print("dir is ", dir, ", offset is : ", offset)

    # print("topic: ", topic, ", minimum offset: ", min)
    return partition_offset


def getoffset(directory, t_c):
    files = glob.glob(directory + "*.index")
    files.sort(key=os.path.getmtime)

    t = os.path.getmtime(files[0])
    a = []
    for f in files[1:]:
        i = getIndexFromFile(f)
        a.append((t, i))
        t = os.path.getmtime(f)

    prev = a[0]
    for t in a[1:]:
        if t[0] < t_c:
            prev = t
    return prev[1]


def getMinimum(partition_offset_map):
    minimum = 0
    for e in partition_offset_map.items():
        if int(e[1]) < minimum:
            minimum = int(e[1])
    return minimum


def printToCSV(consumer_group, topic, minimum):
    with open('output.csv', 'w') as csvfile:
        b = consumer_group + "," + topic + "," + str(minimum)
        csvfile.write(b)
        csvfile.write("\n")


partition_offset_map = get_min_offset_for_date(kafka_logs_dir, topic, epoch_time)
print("partition_offset_map is : ", partition_offset_map)
minimum = getMinimum(partition_offset_map)
printToCSV(consumer_group, topic, minimum)
