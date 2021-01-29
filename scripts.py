#!/usr/bin/env python3


import tempfile
import sys, os

from zipfile import ZipFile
import pandas as pd

import argparse
import shutil
import webbrowser

def create_jsonp(df, meta_col, level):

	    e1 = level
	    e2 = [ col for col in df.columns.to_list() if col not in meta_col][1:]
	    e3 = df.columns.to_list()
	    e4 = df.to_dict("records")
	    jsonp_string = f"load_data( {e1}, {e2}, {e3}, {e4})".replace("'", '"')

	    return jsonp_string
  
def group_level(df, meta_col,level):

	df2 = df.copy()

	df_meta = df2[meta_col].set_index(df2['index'])
	
	feats_col = [ col for col in df.columns.to_list() if col not in meta_col]

	df2 = df2[feats_col]

	df2  = df2.T
	df2, df2.columns = df2[1:] , df2.iloc[0]

	feats_join = df2.index.str.split(";").to_list()

	df2['join_temp_col'] = [ ";".join(f[:level]) for f in feats_join]
	
	df_group = df2.groupby(by="join_temp_col").sum()
	
	df_group = df_group.T

	df_final = pd.concat([df_group, df_meta], axis=1).reset_index()
	
	return df_final

def get_meta_cols(df):
	meta_cols = []
	for col in df.columns.to_list():
		if "Unassigned" in col or "__" in col or "index" in col:
			continue
		meta_cols.append(col)

	
	return meta_cols

def filter_samples(df, exclude_ids=[], include_ids=[], contain_strings=[]):
	#meta_cols = get_meta_cols(df)
	#print (df.columns.to_list())

	if (len(exclude_ids) + len(include_ids) + len(contain_strings) == 0 ) or exclude_ids +  include_ids == ["", ""]:
		return df

	if len(exclude_ids) > 0:
		include_ids = [s for s in df['index'] if s not in exclude_ids]
	if len(contain_strings) > 0:
		#df = df[pd.DataFrame(df['index'].tolist()).isin(contain_strings).any(1).values]
		pass
	df = df[df['index'].isin(include_ids)]

	df = df.loc[:, (df != 0).any(axis=0)]

	

	return df

def beauty_feats(df):

	feats_col = [s for s in df.columns.to_list() if "Unassigned" in s or "__" in s]

	replace_feats = {}

	for feat in feats_col:
		s = feat.split(";")
		level=len(s)

		if level > 2:
			rename_feat = ";".join(s[-2:])
			replace_feats[feat] = rename_feat

	df = df.rename(columns=replace_feats)

	return df
	
def filter_features(df, contain_strings=[], not_contain_strings=[]):
	if len (contain_strings) + len(not_contain_strings) == 0  or contain_strings + not_contain_strings == ["", ""]:
		return df

	meta_cols = get_meta_cols(df)
	feats_col = [ col for col in df.columns.to_list() if col not in meta_cols][1:]

	remain_cols = []
	

	for feat in feats_col:
		get_feat = False
		if len(contain_strings) > 0 or contain_strings != [""]:
			for string in contain_strings:
				
				if string not in feat:			
					continue
				else:
					get_feat = True

		if len(not_contain_strings) > 0 or  not_contain_strings != [""]:
			for string in not_contain_strings:
				if string in feat:
					get_feat = False
				else:
					continue

		if get_feat == False:
			continue

		remain_cols.append(feat)

	df = df[["index"] + remain_cols + meta_cols]
	return df
	

def main(args):
	with ZipFile(args.barplot) as zipObject:
	    listOfFileNames = zipObject.namelist()
	    taxonomy_files = sorted([fileName for fileName in listOfFileNames if ".csv" in fileName])
	    
	    with zipObject.open(taxonomy_files[-1]) as f:
	    	df = pd.read_csv(f, header=0, delimiter=",", comment="#",index_col=False)
	    
	    max_level = 7

	    df = filter_features(df, contain_strings=args.f_contain_strings.split(","), not_contain_strings=args.f_not_contain_strings.split(","))
	    df = filter_samples(df, include_ids=args.s_include_ids.split(","), exclude_ids=args.s_exclude_ids.split(","))
	   
	    
	    meta_cols = get_meta_cols(df)

	    if os.path.exists(args.outpath):
	 
	    	print (f"{args.outpath} is existed, => Remove ")
	    	
	    	exit()



	    shutil.copytree(args.bindir, args.outpath)


	    for level in range(1, max_level+1):
	    	df2 = group_level(df, meta_cols, level)
	    	if args.f_beauty_feats == "yes":
	    		df2 = beauty_feats(df2)
	    	jsonp_string = create_jsonp(df2, meta_cols, level)

	    	print (jsonp_string, file=open(f"{args.outpath}/level-{level}.jsonp", "w"))
	    	df2.to_csv(f"{args.outpath}/level-{level}.csv", index=True, sep=",")

	abs_index_html = os.path.abspath( args.outpath + "/index.html")
	webbrowser.open_new_tab(f"file://{abs_index_html}")
	
def read_params():


	parser = argparse.ArgumentParser(description=f"Plot Features \n Simple usage: {sys.argv[0]} --barplot demo-barplot.qzv --outpath demo")
	parser.add_argument('--barplot', '-t', dest="barplot", help='qiime2 barplot qzv')
	parser.add_argument('--outpath', dest='outpath',
	                    help='output dir')

	parser.add_argument('--bindir', dest='bindir', default = "qiime_assets",
	                    help='css/js qiime assets for barplot')


	parser.add_argument('--f-contains', dest='f_contain_strings', default = "",
	                    help='filterfeatures which contain some strings')
	parser.add_argument('--f-no-contains', dest='f_not_contain_strings', default = "",
	                    help='filterfeatures which NOT contain some strings')
	parser.add_argument('--s-include-id', dest='s_include_ids', default = "",
	                    help='filtersamples which  matched with some ids')
	parser.add_argument('--s-exclude-id', dest='s_exclude_ids', default = "",
	                    help='filtersamples which NOT matched with some ids')

	parser.add_argument('--beauty-feats', dest='f_beauty_feats', 
	                    help='Short name of features')
	
	args = parser.parse_args()

	if len(sys.argv)==1:
	    parser.print_help(sys.stderr)
	    sys.exit(1)
		
	return args


if __name__ == '__main__':
	args = read_params()
	main(args)
