from databricks_api import DatabricksAPI
db = DatabricksAPI(
    host="https:/....",
    token=dbutils.secrets.get(scope = 'your-scope', key = 'your-key')) #I really recommend using keyvault for storing tokens

from datetime import datetime
import json

date=datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

editable_cluster_settings = ['num_workers', 'autoscale', 'cluster_name', 'spark_version', 'spark_conf',
                            'node_type_id', 'driver_node_type_id', 'custom_tags', 'cluster_log_conf', 
                             'spark_env_vars', 'autotermination_minutes', 'enable_elastic_disk']

def get_cluster_settings(cluster_id):
    """[summary]

    Args:
        cluster_id ([type]): [description]

    Returns:
        [type]: [description]
    """
    return db.clusuter.get_cluster(cluster_id = cluster_id)

def get_jobs(cluster_id):
    """[summary]

    Args:
        cluster_id ([type]): [description]

    Returns:
        [type]: [description]
    """
    all_jobs = db.jobs.list_jobs()
    jobs = {}
    for item in all_jobs['jobs']:
        settings = item['settings']
        if 'existing_cluster_id' in settings.keys():
            if settings['existing_cluster_id'] == cluster_id:
                jobs[item['job_id']] = settings
    return jobs

def prepare_new_settings(settings, edits):
    new_settings = settings.copy()
    for key, value in edits.items():
        if key in editable_cluster_settings:
            if 'autoscale' in new_settings.keys():
                option = 'autoscale'
            else:
                option = 'num_workers'

            if key != option:
                del new_settings[option]
            new_settings[key] = value
    return new_settings


def copy_and_change_cluster(cluster_id, **kwargs):
    try:
        settings = get_cluster_settings(cluster_id)
        save_settings('backup', 'cluster', cluster_id, settings)
        new_settings = prepare_new_settings(settings, kwargs)
        db.cluster.edit_cluster(cluster_id, **new_settings)
    except Exception as e:
        print(e)
    finally:
        save_settings('new', 'cluster', cluster_id, new_settings)


def switch_jobs_state(jobs_dict, state):
    """[summary]

    Args:
        jobs_dict ([type]): [description]
        state ([type]): [description]
    """
    if state in ['PAUSED', 'UNPAUSED']:
        for job_id, settings in jobs_dict.items():
            save_settings('backup', 'job', job_id, settings)
            settings['schedule']['pause_status'] = state
            db.jobs.reset_job(job_id = job_id, new_settings = settings)
            save_settings('new', 'job', job_id, settings)
    else:
        print("Wrong state provided")
        
def edit_cluster(cluster_id, **kwargs):
    """[summary]

    Args:
        cluster_id ([type]): [description]
    """
    try:
        cur_settings = get_cluster_settings(cluster_id)
        save_settings('backup', 'cluster', cluster_id, cur_settings)
        new_settings = {key:value for (key,value) in cur_settings.items() if key in editable_cluster_settings}
        for key,value in kwargs.items():
            if key in editable_cluster_settings:
                if 'autoscale' in new_settings.keys():
                    option = 'autoscale'
                else:
                    option = 'num_workers'

                if key != option:
                    del new_settings[option]
                new_settings[key] = value
        db.cluster.edit_cluster(cluster_id, **new_settings)
    except Exception as e:
        print(e)
    finally:
        save_settings('new', 'cluster', cluster_id, cur_settings)


def save_settings(operation, type, id, settings):
    """[summary]

    Args:
        operation ([type]): [description]
        type ([type]): [description]
        id ([type]): [description]
        settings ([type]): [description]
    """
    try:
        data_json = json.dumps({id:settings})
        path = '/mnt/...'+str(date)+'/'+type+'/'+type+'_'+id+'_'+str(date) 
        dbutils.fs.put(path, data_json)
    except Exception as e:
        print(e)

def get_cluster_by_name(name=None):
    clusters = db.cluster.list_clusters()['clusters']
    for cluster in clusters:
        if cluster['cluster_name'] == name:
            return cluster['cluster_id']