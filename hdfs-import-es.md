# es中数据结构

采用nested方式存储企业
```
enterprise
   |--entname
   |--regno
   |--entstatus
   |...
   |--inv
        |-- invId
        |--invtype
        |...
   |--person
        |-- name
        |-- posistion
        |...
```   

# es中保留的字段

## enterprisebaseinfocollect_hdfs_ext_20170529

```
s_ext_nodenum       	string              	                    	 	 
pripid              	string              	                    	 	 
entname             	string              	                    	 	 
regno               	string              	                    	 	 
enttype             	string              	                    	 	 
industryphy         	string              	                    	 	 
industryco          	string              	                    	 	 
abuitem             	string              	                    	 	 
opfrom              	string              	                    	 	 
opto                	string              	                    	 	 
postalcode          	string              	                    	 	 
tel                 	string              	                    	 	 
email               	string              	                    	 	 
esdate              	string              	                    	 	 
apprdate            	string              	                    	 	 
regorg              	string              	                    	 	 
entstatus           	string              	                    	 	 
regcap              	string              	                    	 	 
opscope             	string              	                    	 	 
opform              	string              	                    	 	 
dom                 	string              	                    	 	 
reccap              	string              	                    	 	 
regcapcur           	string              	                    	 	 
forentname          	string              	                    	 	 
country             	string              	                    	 	 
entname_old         	string              	                    	 	 
name                	string              	                    	 	 
ancheyear           	string              	                    	 	 
candate             	string              	                    	 	 
revdate             	string              	                    	 	 
licid               	string -- 当hdfs中licid字段为空时，取credit_code的第9为到第17位              	                    	 	 
credit_code         	string              	                    	 	 
tax_code            	string              	                    	 	 
zspid               	string 
```

## e_pri_person_hdfs_ext_20170529

```
s_ext_nodenum       	string              	                    	 	 
pripid              	string              	                    	 	 
name                	string              	                    	 	 
certype             	string              	                    	 	 
cerno               	string              	                    	 	 
sex                 	string              	                    	 	 
natdate             	string              	                    	 	 
lerepsign           	string              	                    	 	 
country             	string              	                    	 	 
position            	string              	                    	 	 
offhfrom            	string              	                    	 	 
offhto              	string              	                    	 	 
zspid               	string
```

## e_inv_investment_hdfs_ext_20170529

```
s_ext_nodenum       	string              	                    	 	 
pripid              	string              	                    	 	 
invid               	string              	                    	 	 
inv                 	string              	                    	 	 
invtype             	string              	                    	 	 
certype             	string              	                    	 	 
cerno               	string              	                    	 	 
blictype            	string              	                    	 	 
blicno              	string              	                    	 	 
country             	string              	                    	 	 
currency            	string              	                    	 	 
subconam            	string  --认缴出资额             	                    	 	 
acconam             	string              	                    	 	 
conprop             	string  -- 出资比例，计算出来            	                    	 	 
conform             	string              	                    	 	 
condate             	string              	                    	 	 
conam               	string              	                    	 	 
cerno_old           	string              	                    	 	 
zspid               	string 

```
