# study-feast
- https://dailyheumsi.tistory.com/265
- https://github.com/feast-dev/feast-workshop

### install
```shell
pip3 install -r requirements.txt

feast version
```
```shell
feast init asdf

cd asdf

feast apply
ll data
```

### offline
```shell
cd asdf

python3 offline.py
```

### online
```shell
cd asdf

CURRENT_TIME=$(date -u +"%Y-%m-%dT%H:%M:%S")
feast materialize-incremental $CURRENT_TIME

python3 online.py
```
