# -*- coding: utf-8 -*-
#------------------------------------#
# Project Description                #
#------------------------------------#
__dateCreated__= "Mar 17 2023"
__project__= "Geo Entity Stats Server"
__module__= "Geo Entity Stats Server"
__author__= "Nitin Mishra"
__internal_code_reviewer__= "Pankaj Bodani"
__maintain_by__= "Nayan"
__entity__= "CGDD/VRG/EPSA"
__organization__= "SAC/ISRO"
__purpose__= "This submodule is developed for calculating stats for raster parameters to mapped geo_entities"
__execution_mode__= "This submodule can be executed either in manual or automated mode"

#------------------------------------#
# Module Import                      #
#------------------------------------#
import json # To read config json
import pandas as pd #To work on pandas dataframe
import geopandas as gpd # For GeoJSON
import psycopg2 # For PostgreSQL Database Queries
import psycopg2.extras as extras
import sys # For Runtime Environment
import os # OS kernel module access
from rasterstats import zonal_stats #To zonal stats
from sqlalchemy import create_engine,text
from urllib.parse import quote_plus
from datetime import  date, datetime
from time import strptime
import time
import re
from shapely.errors import ShapelyDeprecationWarning
import warnings
warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)
import glob
import rasterio


class GeoEntity_Stats_Generation:
    #------------------------------------#
    # Private Members: Config and Database                    #
    #------------------------------------#    
    __config_file_path=None
    __DB=None
    __conn=None
    __processing_record_chunk=None
    __exit_status=0
       
    #------------------------------------#
    # Aux Methods                        #
    #------------------------------------#
    def sameVal(self, x):
        return x
    
    def category_max(self, data):
        if(data):
            max_obj=max(zip(data.values(), data.keys()))
            returnobj={'category':data,'mode':max_obj[1]}
            return returnobj
        else:
            return {'category':None,'mode':None}
        
   
    def find_dates_from_filename(self,filename):
        date_pattern = re.compile(r'\d{8}')
        dates = date_pattern.findall(filename)
       
        if len(dates) == 2:
            return {"from_date": dates[0], "to_date": dates[1]}
        else:
            return {"from_date": "", "to_date": ""}


    def __printMsg(self,opt,text):
        if opt=="Warning":
            print("\n[Warning]: "+text+"\n")
        elif opt=="Info":
            print("[Info]: "+text)
        elif opt=="Error":
            print("\n<Error> "+text+"\n")
        else:
            print("Unsupported option "+opt+" for prinitng.")
    
    
    def __getDataFrameFromGeoEntitySource(self,geo_entity_table,source_id,region_prefix_filter):
        """
        Purpose
        ----------
        This method will read Database config and will return Geopanda Data Frame.
        
        Parameters
        ----------
        geo_entity_table: name of Geoentity table
        source_id : geoentity_source_id

        Returns
        -------
        GDF : Geo Panda Data Frame
        """
        self.__printMsg("Info","---GeoEntity fetching for GeoEntity_Source_ID:"+str(source_id)+" is started---")
        DataFrame=None
        try:
            entity_filter='|'.join(region_prefix_filter)    
            sql=text("SELECT geoentity_id,geom FROM "+geo_entity_table+" where geoentity_source_id="+str(source_id) +" and geoentity_id similar to '%("+entity_filter+")%'")            
            DataFrame=gpd.read_postgis(sql, self.__DB.connect())
        except:
            self.__exit_status=1
            self.__printMsg("Error", "Sorry unable to give frame from database, plese check DB parameters.")
            sys.exc_info()
            sys.exit(self.__exit_status)
        self.__printMsg("Info","---GeoEntity fetching for GeoEntity_Source_ID:"+str(source_id)+" is completed---")       
        return DataFrame
      
       
     
    def __checkValidFileDates(self,param_file):
       param_file_parts = param_file.split("_")
       dateExtract = param_file_parts[-1].split(".")[0]
       #match=re.search("^[1-2][0-9][0-9][0-9][0-1][0-9][0-3][0-9]$",dateExtract)
       match=re.search("^(?:[1-2][0-9]{3}[0-1][0-9][0-3][0-9]|[1-2][0-9]{3}[0-1][0-9][0-3][0-9][0-2][0-9][0-5][0-9](?:[PN][0-2][0-9][0-5][0-9])?)$",dateExtract)
       if match:
           return True
       else:
           return False


    def __parse_datetime_without_tz(self,date_string):
        length = len(date_string)
        dt=None
        if length == 8:
            # Format: YYYYMMDD
            dt = datetime.strptime(date_string, '%Y%m%d')
        
        elif length >= 12:
            # Format: YYYYMMDDHHMM
            date_string=date_string[:12]
            dt = datetime.strptime(date_string, '%Y%m%d%H%M')
        
        return dt    
	
   
    def __date_object_from_filename(self,param_file):
        param_file_parts = param_file.split("_")
        dateExtract = param_file_parts[-1].split(".")[0]
        d=self.__parse_datetime_without_tz(dateExtract)
        return d
    #    d = datetime.strptime(dateExtract, '%Y%m%d')
    #    return d.year,d.month,d.day
        
    def __date_to_tz_string(self,param_file):
        param_file_parts = param_file.split("_")
        dateExtract = param_file_parts[-1].split(".")[0]       
        
        dt_obj=self.__date_object_from_filename(param_file)

        dt_string=dt_obj.strftime("%Y-%m-%d %H:%M:%S")

        if 'P' in dateExtract:
            dt_string=dt_string+'+'+dateExtract[-4:-2]+dateExtract[-2:]
        elif 'N' in dateExtract:
            dt_string=dt_string+'-'+dateExtract[-4:-2]+dateExtract[-2:]
        else:
            dt_string=dt_string+'+05:30'
        return dt_string

    
    def __ingest_df_values_to_DB_table(self,df, table,update_flag=False):      
        cursor = self.__conn.cursor()
        for stats_data in df.itertuples():
            try:
                insert_qry="INSERT INTO "+table+" (geoentity_id, geoentity_source_id, param_id, stats_value, valtimestamp,auxdata) VALUES ('"+stats_data.geoentity_id+"',"+ str(stats_data.geoentity_source_id)+","+ str(stats_data.param_id)+",'"+ str(stats_data.stats_value)+"','"+ stats_data.valtimestamp+"','"+json.dumps(stats_data.auxdata)+"');"
                cursor.execute(insert_qry)
            except psycopg2.Error as e:                
                if update_flag and "duplicate" in e.pgerror:                   
                    try:
                        update_qry="UPDATE "+table+" SET stats_value='"+ str(stats_data.stats_value)+"' WHERE geoentity_id='"+stats_data.geoentity_id+"' and geoentity_source_id="+ str(stats_data.geoentity_source_id)+" and param_id="+ str(stats_data.param_id)+" and valtimestamp='"+ stats_data.valtimestamp+"'"
                        cursor.execute(update_qry)
                        self.__printMsg("Info","Duplicate record updated with new values.")
                    except (Exception, psycopg2.DatabaseError) as error:
                        self.__printMsg("Error","Ingest issue %s" % error)
                        self.__exit_status=1                        
                else:
                    self.__printMsg("Warning","Ingest issue %s" % e)
        cursor.close()   
    
    def __get_ParamID(self,param_name,param_table):
        cursor = self.__conn.cursor()
        try:
            param_id_qry="SELECT id FROM " +param_table+" WHERE param_name='"+param_name+"'"
            cursor.execute(param_id_qry)
            param_id=cursor.fetchone()[0]
            cursor.close() 
            return param_id
        except (Exception, psycopg2.DatabaseError) as error:
            self.__exit_status=1
            cursor.close()
            return None
           
    
    
    def __gen_geoentity_stats(self,geoentity_param_time_stat_table,geoenity_source_id,geoentity_gdf,param,param_table,file_name_filter=None):
        """
        Purpose
        ----------
        This method will generate stats for given geo_entity.
        
        Parameters
        ----------
        geoentity_gdf: GeoDataFrame which contain geo_entity information.
        param: Information for parameters, for which stats have to be generated.
       
        
        Returns
        -------
        Verbose
        """
        param_name=param["param_name"]
        print("paramsss",param_name)
        param_id=self.__get_ParamID(param_name,param_table)
        param_path=param["folder_path"]
        param_stats=None
        if ("categorical_data" in param) and (param["categorical_data"]==True):
            self.__printMsg("Info","Parameters <"+param_name+">, is having category.")
        else:
            param_stats=param["stats"]
        exclude_vals_for_stats=param["exclude_values_for_stats"]
        update_flag=param["update_flag"]
        process_execution_log_file=open(param_path+"/process_execution_log"+"_"+str(geoenity_source_id)+"_"+str(param_id)+".txt","a+")
        process_execution_log_file.seek(0)
        process_execution_log=process_execution_log_file.read()        
        process_execution_log_file.close()
        try:            
            #param_files = glob.glob(param_path+'/*/*.tif', recursive=True)       
            param_files = glob.glob(os.path.join(param_path, '**', '*.tif'), recursive=True)
            # print("GLOB DECLARED",param_files)  
        except:
            self.__printMsg("Error","Parameters <"+param_name+">, files listing have issue.")
        self.__printMsg("Info","---Stats Generation for Parameter:"+param_name+" is started.---")    
        # try:
        record_chunk=self.__processing_record_chunk
        record_batch=int(len(geoentity_gdf)/record_chunk)
        print("record batch",record_batch)
        record_remainder=len(geoentity_gdf)%record_chunk
        #file_name_filter = param["file_name_filter"]
        for param_file in param_files:        
            #if param_file.endswith(".vrt") or param_file.endswith(".tif"):            
            if (param_file.endswith(".tif") and self.__checkValidFileDates(param_file) and param_id is not None and (file_name_filter in param_file or file_name_filter is None)):
                
                if param_file not in process_execution_log:
                    process_execution_log_file=open(param_path+"/process_execution_log"+"_"+str(geoenity_source_id)+"_"+str(param_id)+".txt","a+")
                    process_execution_log_file.seek(0)                    
                    #param_unixtimestamp= datetime(*self.__generic_date_convertor(param_file)).strftime("%Y-%m-%d 00:00:00+05:30")
                    param_unixtimestamp= self.__date_to_tz_string(param_file)                  
                    param_from_date_to_date_for_aux=self.find_dates_from_filename(param_file)
                    # print('Unix Timestamp :-',param_unixtimestamp,'\nParam_path:-',param_path,'\nparam_file:-',param_file)
                    # print('Record batch',record_batch)
                    # print('Record Remainder:-',record_remainder)
                    ds = rasterio.open(param_file)
                    print("GDF before",geoentity_gdf.crs)                   
                    #geoentity_gdf = geoentity_gdf.set_crs('EPSG:4326',allow_override=True)
                                       
                    print("DS_CRS",ds.crs, ds.transform)
                    geoentity_gdf = geoentity_gdf.to_crs(ds.crs)
                    print("final df",geoentity_gdf)
                    print(f"GeoDataFrame CRS: {geoentity_gdf.crs}")
                    print("Param_stats",param_stats)
                    for i in range(record_batch):
                        print("insdie Param_stats",param_stats)

                        print("i index",i)
                        final_result=None
                        output=None
                        print(f"process batch {i} for parameter {param_file}")
                        # print('Record batch i',i, param)
                        if ("categorical_data" in param) and (param["categorical_data"]==True):
                            print("CATEGORICAL DATA TRUE")
                            # output = pd.DataFrame(zonal_stats(vectors=geoentity_gdf[i*record_chunk:(i+1)*record_chunk],all_touched=True,geojson_out=True,raster=param_path+'/'+param_file,categorical=True))['properties']
                            output = pd.DataFrame(zonal_stats(vectors=geoentity_gdf[i*record_chunk:(i+1)*record_chunk],all_touched=True,geojson_out=True,raster=param_file,categorical=True))['properties']
                        else:
                            print("CATEGORICAL DATA false")
                            output = pd.DataFrame(zonal_stats(vectors=geoentity_gdf[i*record_chunk:(i+1)*record_chunk],all_touched=True,geojson_out=True,raster=param_file,stats=param_stats))['properties']                                                                            
                            param_db_output=[]
                            for i in range(len(output)):
                                geoentity_id=output[i]['geoentity_id']
                                print("GEOentity id",geoentity_id)
                          
                                del output[i]['geoentity_id']
                               # print("stats",output[j])
                           # skip_null=param['skip_null']
                                skip_null=param.get('skip_null',False)
                                print("SKIPPP_NULL",skip_null)
                         
                                value_to_insert=output[i][param_stats[0]]
                                if skip_null is True:
                                        if value_to_insert is None:
                                            print(f"skip for {i} due to null value")
                                            continue
                                print(f"inserting..{value_to_insert} for index{i}")
                            if ("categorical_data" in param) and (param["categorical_data"]==True):                                
                                param_db_output.append( [geoentity_id,geoenity_source_id,param_id,json.dumps(eval(param["categorical_fn"])(output[i])),param_unixtimestamp,param_from_date_to_date_for_aux])                              
                            else:
                                param_db_output.append( [geoentity_id,geoenity_source_id,param_id,json.dumps(output[i]),param_unixtimestamp,param_from_date_to_date_for_aux])     
                            #param_db_output.append( [geoentity_id,geoenity_source_id,param_id,json.dumps(output[i]),param_unixtimestamp])                              
                        final_result=pd.DataFrame(param_db_output,columns=["geoentity_id", "geoentity_source_id","param_id","stats_value","valtimestamp","auxdata"])
                        self.__ingest_df_values_to_DB_table(final_result, geoentity_param_time_stat_table,update_flag)
                    
                    if(record_remainder>0):
                        if ("categorical_data" in param) and (param["categorical_data"]==True):
                            output = pd.DataFrame(zonal_stats(vectors=geoentity_gdf[record_batch*record_chunk:],all_touched=True,geojson_out=True,raster=param_file,categorical=True))['properties']                                                                         
                        else:
                            output = pd.DataFrame(zonal_stats(vectors=geoentity_gdf[record_batch*record_chunk:],all_touched=True,geojson_out=True,raster=param_file,stats=param_stats))['properties']                                                                                                                                       
                        param_db_output=[]
                        for i in range(len(output)):
                            geoentity_id=output[i]['geoentity_id']
                            del output[i]['geoentity_id']

                            skip_null=param.get('skip_null',False)
                            print("SKIPPP_NULL",skip_null)

                            value_to_insert=output[i][param_stats[0]]
                            if skip_null is True:
                                    if value_to_insert is None:
                                        print(f"skip for {i} due to null value")
                                        continue
                            print(f"inserting..{value_to_insert} for index{i}")

                            if ("categorical_data" in param) and (param["categorical_data"]==True):                                
                                param_db_output.append( [geoentity_id,geoenity_source_id,param_id,json.dumps(eval(param["categorical_fn"])(output[i])),param_unixtimestamp,param_from_date_to_date_for_aux])                              
                            else:
                                param_db_output.append( [geoentity_id,geoenity_source_id,param_id,json.dumps(output[i]),param_unixtimestamp,param_from_date_to_date_for_aux])                              
                        final_result=pd.DataFrame(param_db_output,columns=["geoentity_id", "geoentity_source_id","param_id","stats_value","valtimestamp","auxdata"])
                        self.__ingest_df_values_to_DB_table(final_result, geoentity_param_time_stat_table,update_flag)
                    self.__printMsg("Info","Parameter File:"+param_file+" is processed.")
                    process_execution_log_file.write(param_file+"\n")
                    process_execution_log_file.close()
                else:
                    self.__printMsg("Info","Parameter File:"+param_file+" is already processed.")
        # except:
        #     self.__printMsg("Error","Exception Occured While Proessing the log")
        #     self.__exit_status=1            
                   
        self.__printMsg("Info","---Stats Generation for Parameter:"+param_name+" is completed.---")




    
   
    
    #------------------------------------#
    # Execution of Main Methods          #
    #------------------------------------#
    def main(self,config='config.json'):    
        self.__printMsg("Info","======Geo Entity Stats Generation Module is Started.======")
        
        self.__config_file_path=config
        
        #Configuration File Loading
        try:
            config_file=open(self.__config_file_path)
        except:
            self.__exit_status=1
            self.__printMsg('Error', "Sorry config file doesn't exist.")
            sys.exit(self.__exit_status)
        __Config = json.load(config_file)
        config_file.close()
        
        #Global Param Loading      
        host=__Config["global_param"]["database"]["host"]
        username=__Config["global_param"]["database"]["username"]
        password=__Config["global_param"]["database"]["password"]
        port=__Config["global_param"]["database"]["port"]
        db=__Config["global_param"]["database"]["db"]        
        geoentity_table=__Config["global_param"]["database"]["geoentity_table"]
        geoentity_stats_table=__Config["global_param"]["database"]["geoentity_stats_table"]
        param_table=__Config["global_param"]["database"]["param_table"]
        self.__processing_record_chunk = __Config["global_param"]["database"]["processing_record_chunk"]
        
        #Database Connection
        try:
            conn_url="postgresql://"+username+":"+quote_plus(password)+"@"+host+":"+str(port)+"/"+db;
            self.__DB = create_engine(conn_url)
            self.__conn=psycopg2.connect(database=db, user=username, password=password, host=host, port=port)
            self.__conn.autocommit = True     
                
            
        except:
            self.__printMsg("Error"," DB Error, Please check DB Configuration once.")
            self.__exit_status=1
            
            if self.__conn is not None:
                self.__conn.close()
                self.__DB.dispose()
            self.__printMsg('Error',"======Geo Entity Stats Generation Module is failed due to database connection. ======")
            sys.exit(self.__exit_status)
            
        __GeoEntityStatsConfig=__Config["config"]
        
        mapping_type=__GeoEntityStatsConfig["mapping_type"]
        for mapping_key in __GeoEntityStatsConfig["mapping_keys_for_stats_gen"]:            
            mapping_obj=__GeoEntityStatsConfig["mapping"][mapping_type][mapping_key]
            if mapping_type=="entity_mapping":
                source_id=mapping_obj["source_id"]
                region_prefix_filter=mapping_obj["region_prefix_filter"]
                file_name_filter=mapping_obj["file_name_filter"]
                mapping_gdf=self.__getDataFrameFromGeoEntitySource(geoentity_table,source_id,region_prefix_filter)            
                parameters=None
                if(type(mapping_obj["params"])== str):
                    parameters=__GeoEntityStatsConfig["param_template"][mapping_obj["params"]]                                               
                elif (type(mapping_obj["params"])== list):
                    parameters=mapping_obj["params"]
                for param in parameters:                    
                    self.__gen_geoentity_stats(geoentity_stats_table,source_id,mapping_gdf,param,param_table,file_name_filter)
                    # print("PARAM",param)
        
        if self.__conn is not None:            
            self.__conn.close()
            self.__DB.dispose()
        self.__printMsg("Info","======Geo Entity Stats Generation Module is Completed.======")
        sys.exit(self.__exit_status)
 
if __name__ == "__main__":
    Obj=GeoEntity_Stats_Generation()    
    Obj.main(sys.argv[1])
strftime("%Y-%m-%d 00:00:00+05:30")
