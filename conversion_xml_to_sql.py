#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import xml.etree.ElementTree as ET

def replace_filter_expressions(
    filter_str,
    alias,
    target_to_source_map,
    calculated_attribs=None,
    date_func="TO_CHAR(CURRENT_DATE, 'YYYYMMDD')"
):
    """
    Remplace les références (ex: "today", "ChartOfAccounts") dans la chaîne filter_str
    par l'équivalent SQL (ex: TO_CHAR(CURRENT_DATE,'YYYYMMDD'), alias.KTOPL, etc.).

    - filter_str : le contenu texte de la balise <filter>.
    - alias : l'alias SQL (ex: "J3") qu'on doit préfixer aux colonnes.
    - target_to_source_map : dict {target_col: source_col} provenant des <mapping>.
    - calculated_attribs : liste ou dict des attributs calculés (facultatif).
    - date_func : expression SQL pour la date du jour (ex: "TO_CHAR(CURRENT_DATE, 'YYYYMMDD')").

    Retourne la chaîne transformée.
    """
    if not filter_str:
        return ""

    # Remplacement de "today" par la fonction date_func
    # (ex: "TO_CHAR(CURRENT_DATE, 'YYYYMMDD')")
    filter_str = filter_str.replace('"today"', date_func)

    # Pour chaque target => source (ex: "ChartOfAccounts": "KTOPL"),
    # remplacer les guillemets "ChartOfAccounts" par alias.source_col (ex: "J3.KTOPL").
    # Idem pour "DATBI", "DATAB", "SPRAS", etc.
    for target_col, source_col in target_to_source_map.items():
        to_find = f'"{target_col}"'
        replace_with = f"{alias}.{source_col}"
        filter_str = filter_str.replace(to_find, replace_with)

    # Vous pouvez affiner si vous craignez des collisions de chaînes,
    # par ex. utiliser des regex pour n’impacter que "mot complet".
    return filter_str

def process_file(input_file, output_file):
    """
    Lit un fichier SAP HANA Calculation View (fichier XML ou .calculationview) et génère 
    les requêtes SQL correspondantes (JOINs et PROJECTIONS), puis les écrit dans le 
    fichier de sortie spécifié.

    Les éléments suivants sont extraits et transformés en SQL :
        - DataSources (<dataSources>) : permet de construire un dictionnaire de correspondance
        entre les identifiants de sources et la notation "SCHÉMA.TABLE".
        - Vues de jointure (Calculation:JoinView) : génère des clauses de type 
        FROM ... JOIN ... ON ... .
        - Vues de projection (Calculation:ProjectionView) : génère des clauses SELECT 
        et WHERE, en tenant compte de mappings, filtres, colonnes calculées, etc.

    Args:
        input_file (str): 
            Chemin complet vers le fichier XML ou .calculationview à traiter.
        output_file (str):
            Chemin complet du fichier de sortie dans lequel écrire la ou les requêtes SQL.
    
    Étapes principales du traitement:
        1. Parsing du fichier XML avec xml.etree.ElementTree.
        2. Construction d'un dictionnaire des sources de données (id -> "schema.table").
        3. Recherche de la balise <calculationViews> puis, pour chaque <calculationView> :
            - Si c'est un JoinView (xsi_type == 'Calculation:JoinView'):
             * Récupération du joinType (INNER, LEFT, etc.),
             * Assemblage des clauses FROM / JOIN,
             * Sélection et renommage des colonnes en fonction des <mapping>.
            - Si c'est un ProjectionView (xsi_type == 'Calculation:ProjectionView'):
             * Récupération de l'entrée (JOIN antérieur ou table source),
             * Construction des colonnes SELECT à partir des <mapping>,
             * Interprétation des filtres (<filter>) et éventuelle conversion d'éléments spéciaux comme "today".
        4. Construction d'un script SQL final avec la clause WITH, où chaque vue 
            (JoinView ou ProjectionView) est définie sous forme d'alias (ex: MyView AS (...)).
        5. Écriture du script généré dans le fichier spécifié par `output_file`.
    
    Notes:
        - Les types de jointure reconnus sont: "INNER", "LEFTOUTER", "RIGHTOUTER" et "FULLOUTER" (mapping simplifié). Les valeurs inconnues sont traduites par défaut en "INNER JOIN".
        - Les attributs calculés (calculatedViewAttributes) ne sont pas tous explicitement projetés, mais certains interviennent dans les filtres (ex: "today" remplacé par TO_CHAR(CURRENT_DATE, 'YYYYMMDD') ).
        - Si aucune balise <calculationViews> n’est trouvée dans le fichier, aucune requête n’est écrite et un message est affiché.
    """

    tree = ET.parse(input_file)
    root = tree.getroot()
    
    # Si nécessaire, on peut gérer les namespaces (selon votre fichier réel).
    # Ici, on suppose que la résolution de .find() / .findall() fonctionne
    # sans devoir spécifier de namespace. Sinon, vous pouvez ajouter:
    # namespaces = {'Calculation': 'http://www.sap.com/ndb/BiModelCalculation.ecore'}
    # et chercher avec 
    # .findall('.//Calculation:calculationView', namespaces=namespaces)
    
    # Construire un dictionnaire des dataSources: id -> (schemaName, columnObjectName)
    data_sources_dict = {}
    data_sources = root.find('./dataSources')
    if data_sources is not None:
        for ds in data_sources.findall('./DataSource'):
            ds_id = ds.get('id')
            column_obj = ds.find('./columnObject')
            if column_obj is not None:
                schema_name = column_obj.get('schemaName')
                table_name = column_obj.get('columnObjectName')
                data_sources_dict[ds_id] = f"{schema_name}.{table_name}"
    
    # Récupérer toutes les balises <calculationView> sous <calculationViews> 
    calculation_views_parent = root.find('.//calculationViews')
    if calculation_views_parent is None:
        print("Aucune balise <calculationViews> trouvée.")
        return
    
    all_sql_blocks = []  # Pour stocker les morceaux de SQL et les afficher ensuite
    
    for calc_view in calculation_views_parent.findall('./calculationView'):
        # Vérifier qu'il s'agit d'un JoinView
        xsi_type = calc_view.get('{http://www.w3.org/2001/XMLSchema-instance}type')
        view_id = calc_view.get('id', 'UnknownView')
    
        # GESTION DES JOINTURES
        if xsi_type == 'Calculation:JoinView':
            join_type_xml = calc_view.get('joinType', 'inner').upper()
            # On mappe éventuellement d'autres types "leftOuter" => "LEFT JOIN", etc.
            # SAP HANA a parfois des valeurs comme "inner", "leftOuter", "rightOuter".
            # On fait une correspondance simple :
            join_type_map = {
                'INNER': 'INNER JOIN',
                'LEFTOUTER': 'LEFT JOIN',
                'RIGHTOUTER': 'RIGHT JOIN',
                'FULLOUTER': 'FULL JOIN'
                # ajoutez selon vos besoins...
            }
            # Valeur par défaut si non trouvée dans le map
            join_type = join_type_map.get(join_type_xml, 'INNER JOIN')

            # Récupérer la liste des <input>
            inputs = calc_view.findall('./input')

            # Récupérer les attributs de jointure <joinAttribute name="..."/>
            join_attributes = [ja.get('name') for ja in calc_view.findall('./joinAttribute')]

            # Préparer la liste de (alias, tableName, mappings[]) où mappings[] est la liste (source_col, target_col).
            input_info = []
            for i, inp in enumerate(inputs):
                node_ref = inp.get('node')  # ex: "#CSKB"
                if not node_ref:
                    continue
                ds_id = node_ref.lstrip('#') # retire le "#" devant
                alias = f"T{i+1}"
                table_fullname = data_sources_dict.get(ds_id, ds_id)

                # Mappings
                # <mapping xsi:type="Calculation:AttributeMapping" target="MANDT" source="MANDT" />
                mapping_pairs = []
                for mapping in inp.findall('./mapping'):
                    source_col = mapping.get('source')
                    target_col = mapping.get('target')
                    if source_col and target_col:
                        mapping_pairs.append((source_col, target_col))
                
                input_info.append((alias, table_fullname, mapping_pairs))

            # Si pas d'inputs : rien à faire
            if not input_info:
                continue

            # Construire la liste des colonnes à sélectionner dans la clause SELECT
            # On concatène tous les mappings de chaque input, en faisant T_i.source AS target
            select_columns = []
            for (alias, _, map_list) in input_info:
                for (src, tgt) in map_list:
                    if src == tgt:
                        select_columns.append(f"{alias}.{src}")
                    else:
                        select_columns.append(f"{alias}.{src} AS {tgt}")

            # Petite protection : si la liste est vide, on peut aussi 
            # chercher <viewAttributes> pour avoir la liste "à la main".
            # Dans l'exemple donné, on a un mix <viewAttribute id="..."/>. 
            # Mais le plus fiable est de se baser sur les mappings <mapping>.
            # On supposera ici que les mapping <mapping> reflètent bien ce qui sera sélectionné.
            
            # Construire la clause FROM / JOIN
            #    - On prend le premier input comme table de base (FROM)
            #    - On joint les autres inputs un à un avec <joinType> + ON ...
            #    - On utilise <joinAttribute> pour construire la condition :
            #      T1.<joinAttr> = T2.<joinAttr> = T3.<joinAttr> ...
            #      => On peut en pratique faire T1.col = T2.col AND T1.col = T3.col, etc.

            if len(input_info) == 1:
                # S'il n'y a qu'une table, pas de jointure
                from_clause = f"FROM {input_info[0][1]} AS {input_info[0][0]}"
                join_clauses = []
            else:
                # On va enchaîner les joins
                # Base = (première table)
                base_alias, base_table, _ = input_info[0]
                from_clause = f"FROM {base_table} AS {base_alias}"

                join_clauses = []
                
                # Pour chaque table supplémentaire, on construit la condition ON
                for j in range(1, len(input_info)):
                    current_alias, current_table, _ = input_info[j]

                    # Condition ON => pour chaque joinAttribute => T1.attr = T(j+1).attr
                    # Mais pour les inputs > 2, on veut aussi T1.attr = T3.attr...
                    # On va faire la jointure par rapport au "base_alias" (T1),
                    # et potentiellement T2, T3, etc. Tous reliés à T1.  
                    # Dans certains modèles, on joint T2 avec T1, T3 avec T2... 
                    # Mais l’exemple SAP montre T1=MANDT=T2, T1=KOKRS=T2, etc.
                    # Pour la simplicité, on joint tout sur T1.<attr> = T_{j+1}.<attr>.

                    conditions = []
                    for ja in join_attributes:
                        conditions.append(f"{base_alias}.{ja} = {current_alias}.{ja}")

                    on_clause = " AND ".join(conditions)
                    join_clause = f"{join_type} {current_table} AS {current_alias} ON {on_clause}"
                    join_clauses.append(join_clause)
            
            # Construction finale
            join_query_lines = []
            join_query_lines.append(f"{view_id} AS (")
            join_query_lines.append("    SELECT")
            join_query_lines.append(f"        {',\n        '.join(select_columns)}")
            join_query_lines.append(f"    {from_clause}")
            for jc in join_clauses:
                join_query_lines.append(f"    {jc}")
            join_query_lines.append("),")

            # Stocker dans la liste
            all_sql_blocks.append("\n".join(join_query_lines))

        # GESTION DES PROJECTIONS
        elif xsi_type == 'Calculation:ProjectionView':
            # On récupère d’abord l’<input>, par ex. node="#Join_3" => FROM Join_3 AS J3
            projection_inputs = calc_view.findall('./input')
            
            if not projection_inputs:
                continue

            # On suppose qu'il n'y a qu'un seul <input> dans un ProjectionView "classique" (SAP HANA).
            # S'il y en a plusieurs, on pourrait adapter la logique.
            proj_input = projection_inputs[0]
            node_ref = proj_input.get('node')  # ex: "#Join_3"
            node_id = node_ref.lstrip('#')      # "Join_3"

            # Alias : ex: "J1"
            alias = "J1"  # ou "P1", "P2", etc. On peut faire T1, T2...
            # Mappings
            projection_mappings = []
            for mapping in proj_input.findall('./mapping'):
                src = mapping.get('source')
                tgt = mapping.get('target')
                if src and tgt:
                    projection_mappings.append((src, tgt))
            
            # Récupérer <viewAttributes> pour éventuellement lister la totalité des colonnes de sortie
            # On se basera surtout sur le mapping pour la sélection.
            view_attrs = calc_view.find('./viewAttributes')
            # Récupération (facultative) des IDs s’il n’y a pas de mapping, mais la plus part du temps SAP HANA
            # utilise <mapping> pour projeter les colonnes.
            
            # Colonne SELECT
            select_columns = []
            # Crée un dict target->source pour plus tard (WHERE clause)
            target_to_source_map = {}
            for (src, tgt) in projection_mappings:
                target_to_source_map[tgt] = src  # ex: 'ChartOfAccounts' -> 'KTOPL'
                if src == tgt:
                    select_columns.append(f"{alias}.{src}")
                else:
                    select_columns.append(f"{alias}.{src} AS {tgt}")

            # Gérer <calculatedViewAttributes>
            # Exemple: "today", "DATAB_Date", "DATBI_Date"
            # Dans la requête finale SAP HANA, ces attributs peuvent être gérés de diverses manières.
            # L’exemple donné les utilise dans le filtre => "today" <= "DATBI" ...
            # On ne va pas forcement les SELECTer explicitement.
            calc_attrs = calc_view.find('./calculatedViewAttributes')
            calculated_attribs = {}
            if calc_attrs is not None:
                for cva in calc_attrs.findall('./calculatedViewAttribute'):
                    cva_id = cva.get('id')
                    cva_formula = cva.findtext('./formula')
                    calculated_attribs[cva_id] = cva_formula

            # FROM
            from_clause = f"FROM {node_id} AS {alias}"

            # WHERE : basé sur le <filter>
            where_clause = ""
            filter_node = calc_view.find('./filter')
            if filter_node is not None:
                filter_expr = filter_node.text.strip()
                # Remplacer "today" par "TO_CHAR(CURRENT_DATE, 'YYYYMMDD')" etc.
                # Remplacer "ChartOfAccounts" => "J1.KTOPL", etc.
                # (i.e. on applique replace_filter_expressions)
                filter_expr_sql = replace_filter_expressions(
                    filter_expr,
                    alias,
                    target_to_source_map,
                    calculated_attribs=calculated_attribs,
                    date_func="TO_CHAR(CURRENT_DATE, 'YYYYMMDD')"  # ex. Oracle/HANA style
                )
                if filter_expr_sql:
                    where_clause = f"WHERE\n        {filter_expr_sql}"

            # Construction finale
            projection_lines = []
            projection_lines.append(f"{view_id} AS (")
            projection_lines.append("    SELECT")
            projection_lines.append(f"        {',\n        '.join(select_columns)}")
            projection_lines.append(f"    {from_clause}")
            if where_clause:
                projection_lines.append(f"    {where_clause}")
            projection_lines.append("),")

            all_sql_blocks.append("\n".join(projection_lines))
            
    
    if not all_sql_blocks:
        print(f"[{input_file}] Aucune définition de CalculationView exploitable trouvée.")
        return
    else:
        sql_query = "WITH\n\n" + "\n\n".join(all_sql_blocks)
        print(f"[{input_file}] Requêtes SQL générées avec succès.")
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(sql_query)


if __name__ == '__main__':
    import sys
    import os
    import argparse

    parser = argparse.ArgumentParser(description="Génère des requêtes SQL à partir de vues HANA Calculation.")
    parser.add_argument(
        "input_path",
        help="Chemin vers un fichier (.xml | .calculationview) ou un répertoire contenant ces fichiers."
    )
    parser.add_argument(
        "--output",
        help="Chemin de sortie (fichier si input_path est un fichier, répertoire si input_path est un répertoire).",
        default=None
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Parcours récursif des sous-répertoires si input_path est un répertoire."
    )
    args = parser.parse_args()

    # Vérification si input_path est un fichier
    if os.path.isfile(args.input_path):
        # Vérifier l'extension
        ext = os.path.splitext(args.input_path)[1].lower()
        if ext not in [".xml", ".calculationview"]:
            print("Le fichier doit être une extension .xml ou .calculationview.")
            sys.exit(1)

        # Déterminer le chemin de sortie
        if args.output:
            # Si l'utilisateur a fourni un répertoire pour output
            if os.path.isdir(args.output):
                base_name = os.path.splitext(os.path.basename(args.input_path))[0]
                out_path = os.path.join(args.output, base_name + ".sql")
            else:
                # Sinon, c'est un fichier direct
                out_path = args.output
        else:
            # Par défaut, même répertoire, même nom de base
            out_path = os.path.splitext(args.input_path)[0] + ".sql"

        process_file(args.input_path, out_path)

    # Vérification si input_path est un répertoire
    elif os.path.isdir(args.input_path):
        for root, dirs, files in os.walk(args.input_path):
            for filename in files:
                ext = os.path.splitext(filename)[1].lower()
                if ext in [".xml", ".calculationview"]:
                    in_file = os.path.join(root, filename)
                    rel_path = os.path.relpath(in_file, args.input_path)
                    base_name = os.path.splitext(rel_path)[0]  # on enlève l’extension

                    if args.output:
                        # On réplique la structure des dossiers dans args.output
                        out_file = os.path.join(args.output, base_name + ".sql")
                        os.makedirs(os.path.dirname(out_file), exist_ok=True)
                    else:
                        # Même dossier que l’entrée
                        out_file = os.path.join(root, base_name + ".sql")

                    process_file(in_file, out_file)

            if not args.recursive:
                # Si --recursive n'est pas demandé, on ne descend pas dans les sous-dossiers
                break

    else:
        print("Le chemin spécifié n'existe pas ou n'est ni un fichier ni un répertoire.")
        sys.exit(1)