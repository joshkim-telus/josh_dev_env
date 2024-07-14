# global import
import pandas as pd
import numpy as np


def build_output_dataframe(
    df: pd.DataFrame,
    d_data_config: dict
) -> pd.DataFrame:
    """
    Builds the output dataframe based on the input dataframe and data configuration.

    Args:
        df (pd.DataFrame): The input dataframe.
        d_data_config (dict): The data configuration dictionary.

    Returns:
        pd.DataFrame: The output dataframe.

    """
    
    ##########
    # Rank main target variables
    ##########
    # extract main target names from data config dictionary
    l_targets = [target['name'] for target in d_data_config['target_variables']]
    print(f'Ranking {len(l_targets)} main variables')

    # build target name - index mapping from targets
    d_target_idx_mapping = {
        idx: target_name
        for idx, target_name in enumerate(l_targets)
    }
    
    # build reco rank columns from targets
    l_recos = [f'reco_{i}' for i, _ in enumerate(l_targets)]
    
    # sort scores of target variables in descending order
    np_scores = df[l_targets].to_numpy()
    target_ranked = np.argsort(-np_scores, axis=1)    
    df[l_recos] = target_ranked
    
    # loop over reco columns and map idx to target names
    for reco in l_recos:
        df[reco] = df[reco].map(d_target_idx_mapping)
        
    ##########
    # Add combos:
    #  - with 3 products -> check top 3 recos
    #  - with 2 products -> check top 2 recos
    ##########
    print('Adding combos')
    # extract distinct number of products in combos
    unique_number_of_product_in_combos = set([
        len(combo['target_variables']) for combo in d_data_config['combos']
    ])

    # extract unique name of produts in combos
    unique_combo_target_names = set([
        target 
        for combo in d_data_config['combos'] for target in combo['target_variables']
    ])
    
    # check if product is in top n, for each product in combos
    for target_name in unique_combo_target_names:
        for n in unique_number_of_product_in_combos:
            df[f'is_{target_name}_in_top_{n}'] = (df[l_recos[:n]] == target_name).any(axis=1)
            
    # loop over combos
    for combo in d_data_config['combos']:
        print(f"Combo: {combo['name']}")
        
        # set n to check for recommendation combo
        n = len(combo['target_variables'])

        # extract columns to check condition
        l_columns_to_check_combo_condition = [
            f'is_{target_name}_in_top_{n}'
            for target_name in combo['target_variables']
        ]

        # define condition: all targets must be in top n recos
        #condition = df[l_columns_to_check_combo_condition].all(axis=1)
        
        """
        Combo Condition changed:
            - Generate combo scores for all customers
            - Combo score: exctract max score of products on combo 
        """         
        condition = True

        # check condition: if condition pass, return max scores of targets 
        df[combo['name']] = np.where(
            condition, df[combo['target_variables']].max(axis=1), 0
        )
        
    ##########
    # Rank scores again with combos included
    ##########
    # extract combo target names from data config dictionary
    l_combo_targets = [combo['name'] for combo in d_data_config['combos']]
    print(f'Ranking {len(l_combo_targets)} combos variables')

    # aggregate combos and main target names
    # order is important, so combos can be ranked fisrt when score
    # of combo is equal to the highest score of main targets
    l_targets_with_combos = l_combo_targets + l_targets
    print(f'Ranking {len(l_targets_with_combos)} total combos and main variables')
    
    # rebuild target name - index mapping from targets including combos
    d_target_idx_mapping_with_combos = {
        idx: target_name
        for idx, target_name in enumerate(l_targets_with_combos)
    }

    # rebuild reco rank columns from targets including combos
    l_recos_with_combos = [f'reco_{i}' for i, _ in enumerate(l_targets_with_combos)]

    # sort scores of combos and target variables in descending order
    np_scores = df[l_targets_with_combos].to_numpy()
    target_ranked = np.argsort(-np_scores, axis=1)
    df[l_recos_with_combos] = target_ranked
    
    # loop over reco columns and map idx to combo and target names
    for reco in l_recos_with_combos:
        df[reco] = df[reco].map(d_target_idx_mapping_with_combos)
        
    ##########
    # Explode columns to match output table format
    ##########
    # extract columns intersection from data config dictionary and current dataframe
    l_intersection_cols = [
        col for col in d_data_config['output_columns']
        if col in df.columns
    ]

    # extract unique tier names from data config dictionary
    unique_tier_names = set([
        tier['name']
        for tiers in d_data_config['tiers'].values() for tier in tiers
    ])
    
    l_dfs = []
    # loop over recommendations and build output dataframe
    for i, reco in enumerate(l_recos_with_combos):
        print(f'Processing {reco}')

        # build helper dataframe
        df_helper = df[
            l_intersection_cols + [reco] + list(unique_tier_names)
        ].copy()

        # set rank and rename reco column
        df_helper['rank'] = i + 1     
        df_helper = df_helper.rename(columns={reco: 'product_name'})

        # extract model scores
        df_helper['score'] = df.lookup(df_helper.index, df_helper['product_name'])

        l_dfs.append(df_helper)

    df_concat = pd.concat(l_dfs)
    print(f'Concat dataframe df.shape {df_concat.shape}')
    
    # remove rows with score zero, usually combos
    df_concat = df_concat[df_concat['score'] > 0]   
    print(f'Concat dataframe without zero scores df.shape {df_concat.shape}')
    
    ##########
    # Add tiers
    ##########
    # create a new column to store tier results
    df_concat['product_name_tier'] = df_concat['product_name']

    # loop over tiers
    for tier_name, l_tiers_values in d_data_config['tiers'].items():

        # set conditions to edit dataframe
        conditions = (df_concat['product_name'] == tier_name)
        print(f'Processing tier: {tier_name}')

        # loop over tier targets
        for i, d_tier in enumerate(l_tiers_values):
            print(f"Tier: {d_tier['name']}")

            # first iteration: only add tier scores to column
            if i == 0:
                df_concat['product_name_tier'] = np.where(
                    conditions, d_tier['name'], df_concat['product_name_tier']
                )
                df_concat['tier_score'] = np.where(
                    conditions, df_concat[d_tier['name']], 0
                )

            # update dataframe if new tier score is higher than previous tier scores
            else:
                update_conditions = conditions & (df_concat[d_tier['name']] > df_concat['tier_score'])
                df_concat['product_name_tier'] = np.where(
                    update_conditions, d_tier['name'], df_concat['product_name_tier']
                )
                df_concat['tier_score'] = np.where(
                    update_conditions, df_concat[d_tier['name']], df_concat['tier_score']
                ) 
                
    # set tier zeros scores to None
    conditions = (df_concat['tier_score'] != 0)
    df_concat['tier_score'] = np.where(
        conditions, df_concat['tier_score'], None
    )
                            
    ##########
    # Map target names to their abbreviation name
    ##########
    print('Mapping target name with reco name')
    # extract main target name - reco mapping
    d_main_target_reco_mapping = {
        target['name']: target['reco']
        for target in d_data_config['target_variables']
    }

    # extract tier target name - reco mapping
    d_tier_target_reco_mapping = {
        tier['name']: tier['reco']
        for tiers in d_data_config['tiers'].values() for tier in tiers
    }

    # extract combos target name - reco mapping
    d_combo_target_reco_mapping = {
        combo['name']: combo['reco']
        for combo in d_data_config['combos']
    }

    # create final mapping dictionary
    d_target_reco_mapping = {}
    for d_map in (d_main_target_reco_mapping, d_tier_target_reco_mapping, d_combo_target_reco_mapping):
        d_target_reco_mapping.update(d_map)
        
    # map names and recos
    df_concat['reco'] = df_concat['product_name_tier'].map(d_target_reco_mapping)
    
    # select output columns
    df_output = df_concat[d_data_config['output_columns']]
    df_output['product_name'] = df_concat['product_name_tier']
    print(f'Final dataframe df.shape {df_output.shape}')
    
    return df_output
