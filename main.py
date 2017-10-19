import csv
import hashlib
import re
import random,math
import sklearn_crfsuite
from sklearn_crfsuite import scorers
from sklearn_crfsuite import metrics

data_path = "./NASA_access_log_Jul95"

users = {}
line_patt = r'([^\s]+) .+ .+ \[(.*)\] \"([^\"]+)\" (.+) (.+)'  #\[([^\]+)\] \"([^\"]+)\" ([\d]+) ([\d]+)
count = 0
with open(data_path, 'rb') as access_log:
    #access_log = csv.reader(csvfile, delimiter=' ', quotechar='[')
    for row in access_log:
        m = re.match(line_patt, row)
        if m:
            row = m.groups()
            
            host = row[0]
            timestamp = row[1]
            request = row[2]
            status = row[3]
            bytes_sent = row[4]

            user = hashlib.md5(host)
            user = host
            parts = request.split(" ")
            if (len(parts)>1):
                method = parts[0]
                path = parts[1]
                request = {
                    "path": path,
                    "timestamp": timestamp,
                    "status": status,
                    "bytes_sent":bytes_sent
                }
                if (path.endswith("/") or path.endswith(".html")):
                    if user not in users:
                        users[user] = []
                        previous_request = {}
                    else:
                        previous_request = users[user][-1]
                    if "path" in previous_request:
                        request['-1path'] = previous_request['path']
                    else:
                        request['-1path'] = None
                    
                    users[user].append(request)
                    count +=1
        if (count == 7000):
            break;

def to_features_labels(users):
    features, labels = [], []
    for user in users:
        doc = []
        doc_labels = []
        visits = users[user]
        for a in range(0,len(visits)):
            next = a + 1
            v = {
                "path": visits[a]['path'],
                "-1path": visits[a]['-1path'] or ""
            }
            doc.append(v)
            if next<len(visits):
                doc_labels.append(visits[next]['path'])
            else:
                doc_labels.append("end")
        features.append(doc)
        labels.append(doc_labels)
    return features, labels


count = len(users)
train_size = int(math.floor(count * 0.7))
usernames = users.keys()
random.shuffle(usernames)
_train = usernames[:train_size]
_test = usernames[train_size+1:]

#x_train, y_train = to_features_labels(_train)

#train = [visit for i in _train for visit in users[i]]
train = { user: users[user] for user in _train }
test = { user: users[user] for user in _test }

print count, len(train), len(test)

x_train, y_train = to_features_labels(train)
x_test, y_test = to_features_labels(test)

print len(x_train), len(y_train)
print x_train[0]

#%%time
crf = sklearn_crfsuite.CRF(
    algorithm='lbfgs',
    c1=0.1,
    c2=0.1,
    max_iterations=100,
    all_possible_transitions=True
)
crf.fit(x_train, y_train)

labels = list(crf.classes_)
print labels

y_pred = crf.predict(x_test)
#metrics.flat_f1_score(y_test, y_pred, average='weighted', labels=labels)

# group B and I results
sorted_labels = sorted(
    labels,
    key=lambda name: (name[1:], name[0])
)
print(metrics.flat_classification_report(
    y_test, y_pred, labels=sorted_labels, digits=3
))

# features, labels = to_features_labels(users)

# print len(features), len(labels)

# for a in range(0,len(features)):
#     print features[a], labels[a]

# print len(users.keys())
# print count
# #print users[users.keys()[100]]

# dict_flattened = [ visit for user in users.values() for visit in user]
# print dict_flattened[:5]