(base) shenfei@ML01:~/gitlab_master$ crontab -e

# indicating with different fields when the task will be run
# and what command to run for the task
# 
# To define the time you can provide concrete values for
# minute (m), hour (h), day of month (dom), month (mon),
# and day of week (dow) or use '*' in these fields (for 'any').# 
# Notice that tasks will be started based on the cron's system
# daemon's notion of time and timezones.
# 
# Output of the crontab jobs (including errors) is sent through
# email to the user the crontab file belongs to (unless redirected).
# 
# For example, you can run a backup of all your user accounts
# at 5 a.m every week with:
# 0 5 * * 1 tar -zcf /var/backups/home.tgz /home/
# 
# For more information see the manual pages of crontab(5) and cron(8)
# 
# m h  dom mon dow   command
# shenfei check new data and copy the new data for recommendation twice at 9:00 and 18:00 everyday
0 9 * * * python3 ~/gitlab_master/my_crontab/my_crontab.py >> ~/gitlab_master/my_crontab/crontab.log 2>&1
0 18 * * * python3 ~/gitlab_master/my_crontab/my_crontab.py >> ~/gitlab_master/my_crontab/crontab.log 2>&1

# shenfei start recommendation training at 20:00 everyday
0 20 * * * time bash ~/gitlab_master/scripts/taskrunner.sh >"/home/shenfei/gitlab_master/log/`date +"\%Y\%m\%d"`_formal.log" 2>&1

# shenfei POST .bson file to test environment & formal environment at 10:00 & 10:08 everyday
5 10 * * * curl -i -X POST -H "Content-Type: multipart/form-data" -F "file=@/home/shenfei/gitlab_master/recommendation/userResults.json.gz" http://47.110.200.89:8880/upload2 >> ~/gitlab_master/my_crontab/crontab.log 2>&1
0 10 * * * curl -i -X POST -H "Content-Type: multipart/form-data" -F "file=@/home/shenfei/gitlab_master/recommendation/userResults.json.gz" http://backstage-test.yc345.tv:3009/upload2 >> ~/gitlab_master/my_crontab/crontab.log 2>&1
~                