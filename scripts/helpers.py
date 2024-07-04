import pandas as pd
import json

variable_paths = pd.read_csv('json_paths/data_variables.csv')
included_paths = pd.read_csv('json_paths/included_variables.csv')

size_ranges = {(None, 10): 0, (11, 50): 1, (51, 200): 2, (201, 500): 3, (501, 1000): 4, (1001, 5000): 5, (5001, 10000): 6, (10001, None): 7}

def strip_val(val, cat):
    if cat == 0 or val is None:
        return val
    elif cat == 1:
        return val.split(':')[-1]
    elif cat == 2:
        return val.split('.')[-1]
    else:
        raise ValueError

def get_value_by_path(dictionary, path):
    keys = path.strip("[]'").split("']['")
    for key in keys:
        if not dictionary or key not in dictionary:
            return None
        dictionary = dictionary[key]
    return dictionary

def process_raw_description(raw_description):
    if not raw_description:
        return ""
    
    text = raw_description.get('text', '')
    attributes = raw_description.get('attributes', [])
    
    attributes.sort(key=lambda x: x['start'])
    
    formatted_text = []
    last_end = 0
    in_list = False
    processed_ranges = set()
    
    for attr in attributes:
        start = attr['start']
        length = attr['length']
        end = start + length
        attr_type = list(attr['attributeKindUnion'].keys())[0] if 'attributeKindUnion' in attr else attr['type'].get('$type', '').split('.')[-1]
        
        if last_end < start:
            formatted_text.append(text[last_end:start])
        
        if (start, end) not in processed_ranges:
            if attr_type == 'bold':
                formatted_text.append(f'**{text[start:end]}**\n\n')
            elif attr_type == 'paragraph':
                formatted_text.append(f'{text[start:end]}\n\n')
            elif attr_type == 'lineBreak':
                formatted_text.append('\n')
            elif attr_type == 'listItem':
                if not in_list:
                    formatted_text.append('\n')
                    in_list = True
                formatted_text.append(f'• {text[start:end].strip()}\n')
            elif attr_type == 'list':
                in_list = True
            else:
                formatted_text.append(text[start:end])
            
            processed_ranges.add((start, end))
        
        last_end = max(last_end, end)
    
    if last_end < len(text):
        formatted_text.append(text[last_end:])
    
    # Clean up extra newlines
    result = ''.join(formatted_text)
    result = result.replace('\n\n\n', '\n\n')
    result = result.strip()
    
    return result


def clean_job_postings(all_jobs):
    all_cleaned_postings = dict()
    for job_id, job_info in all_jobs.items():
        # print(f"{job_info=}")
        if job_info == -1:
            posting = {'error': job_info}
        else:
            posting = {
                'jobs': {}, 'companies': {}, 'salaries': {}, 'benefits': {}, 
                'industries': {}, 'skills': {}, 'employee_counts': {}, 
                'company_industries': {}, 'company_specialities': {}
            }
            
            for _, row in variable_paths.iterrows():
                value = get_value_by_path(job_info, row['path'])
                if value is not None:
                    posting[row['table']][row['name']] = strip_val(value, row['strip'])

            # Process specific fields
            if 'description' in posting['jobs']:
                posting['jobs']['description'] = process_raw_description(job_info['data']['description'])
            
            if 'skill_abrs' in posting['skills'] and 'skill_name' in posting['skills']:
                posting['skills'] = dict(zip(posting['skills']['skill_abrs'], posting['skills']['skill_name']))
            
            if 'industry_ids' in posting['industries'] and 'industry_names' in posting['industries']:
                posting['industries'] = dict(zip(posting['industries']['industry_ids'], posting['industries']['industry_names']))

            # Handle included data
            for _, row in included_paths.iterrows():
                for included_item in job_info.get('included', []):
                    if strip_val(included_item.get('$type'), 2) == row['type']:
                        if row['name'] == 'company_size':
                            company_size_info = get_value_by_path(included_item, row['path'])
                            if company_size_info:
                                posting[row['table']][row['name']] = size_ranges.get((company_size_info.get('start'), company_size_info.get('end')))
                        else:
                            value = get_value_by_path(included_item, row['path'])
                            posting[row['table']][row['name']] = strip_val(value, row['strip'])

        all_cleaned_postings[job_id] = posting
    try:
        print(f"{posting['jobs']['description']=}")
    except Exception as e:
        print(e)
        print(posting)
    # print(all_cleaned_postings)

    return all_cleaned_postings
