# BeansDB Admin

## 部署

旧机房
  doubandb http://dba1:5000/
  doubanfs  http://dba1:5010/
  
新机房

  doubandb http://cotman1:5000/
  doubanfs  http://cotman1:5010/


puppet 相关配置

modules/beansdb/manifests/doubandb_admin.pp
modules/beansdb/manifests/doubanfs_admin.pp

## 安装与运行

```shell
pip install git+https://github.com/douban/beansdbadmin.git@master#egg=beansdbadmin
beansdbadmin-server --cluster db256 --port 9999
```

```
sudo emerge flask
sudo  mkdir /opt/beansdbadmin/
sudo chown beansdb:beansdb /opt/beansdbadmin/ 
sudo mkdir /var/log/beansdb-admin/
sudo chown beansdb:beansdb /var/log/beansdb-admin/

sudo -u beansdb /bin/bash

cd /opt/beansdbadmin/
git clone https://github.intra.douban.com/coresys/beansdbadmin.git
cd /opt/beansdbadmin/beansdbadmin 
export PYTHONPATH="$PYTHONPATH:/opt/beansdbadmin/beansdbadmin"
python /opt/beansdbadmin/beansdbadmin/beansdbadmin/tools/gc.py -i  -c db256

# python beansdbadmin/index.py --cluster fs --port 5010 # test!
```

```
node 'cotman1.intra.douban.com' {
    include beansdb::doubanfs_admin
    include beansdb::doubandb_admin
}
```


  
  直接在 dba1 上 ```/opt/beansdbadmin/beansdbadmin```  git pull
  
  相关服务:
  
  ```
  /service/beansdb-admin-web-db 
   /service/beansdb-admin-web-fs
  /etc/cron.d/doubandb-autogc 
  ```
