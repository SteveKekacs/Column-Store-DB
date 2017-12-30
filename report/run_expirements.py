"""
Simple python script to run various expirements.
"""
import os
import sys
import time
import random

create_db = 'create(db,"db1")\n'


def run_script(script):
    # write script to tmp file
    f = open("tmp.dsl", "w")
    f.write(script)
    f.close()

    # run script
    start = time.time()
    os.system("./client < tmp.dsl")
    end = time.time()
    return int((end - start) * 1000000)


def generate_scans_lines(num_scans = 1):
    # min and max vals in columns
    min_val = 0
    max_val = 50000

    individual_ret = ""
    shared_ret = "batch_queries()\n"

    scan_template = "s{0}=select(db1.tbl3_batch.col1,{1},{2})\n"

    for i in range(num_scans):
        val1 = random.randint(min_val, min_val + 500)
        val2 = random.randint(max_val - 25000, max_val)
        scan_line = scan_template.format(i, val1, val2)
        individual_ret += scan_line

    shared_ret += individual_ret
    shared_ret += "batch_execute()\n"

    return individual_ret, shared_ret


def run_shared_individual_expirement():
    # compile setup lines
    setup = create_db
    lines = [
        'create(tbl,"tbl3_batch",db1,4)',
        'create(col,"col1",db1.tbl3_batch)',
        'create(col,"col2",db1.tbl3_batch)',
        'create(col,"col3",db1.tbl3_batch)',
        'create(col,"col4",db1.tbl3_batch)',
        'load("/home/ec2-user/project/project_tests_1M/data3_batch.csv")',
    ]
    setup += "\n".join(lines) + "\n"

    max_num_scans = 100

    # build
    os.system("make clean > /dev/null")
    os.system("make > /dev/null")

    # run server
    print("Running server...")
    os.system("./server > server.out &")
    time.sleep(1)

    # run setup
    print("Running setup...")
    run_script(setup)

    print("Running expirement...")
    results = {
        "individual": {},
        "shared": {}
    }
    for i in range(max_num_scans):
        print("Testing %d scans..." % (i + 1))
        individual_script, shared_script = generate_scans_lines(i + 1)

        # run and time each script
        ind_time = run_script(individual_script)
        time.sleep(1)
        shared_time = run_script(shared_script)
        
        results["individual"][i + 1] = ind_time
        results["shared"][i + 1] = shared_time

    # kill server
    os.system("pkill -f './server'")

    return results


def run_shared_scans_expirement():
    # compile setup lines
    setup = create_db
    lines = [
        'create(tbl,"tbl3_batch",db1,4)',
        'create(col,"col1",db1.tbl3_batch)',
        'create(col,"col2",db1.tbl3_batch)',
        'create(col,"col3",db1.tbl3_batch)',
        'create(col,"col4",db1.tbl3_batch)',
        'load("/home/ec2-user/project/project_tests_1M/data3_batch.csv")',
    ]
    setup += "\n".join(lines) + "\n"

    max_num_scans = 100

    # build
    os.system("make clean > /dev/null")
    os.system("make > /dev/null")

    # run server
    print("Running server...")
    os.system("./server > server.out &")
    time.sleep(1)

    # run setup
    print("Running setup...")
    run_script(setup)

    # generate scripts
    print("Generating scripts...")
    scripts = []
    for i in range(max_num_scans):
        _, shared_script = generate_scans_lines(i + 1)
        scripts.append(shared_script)

    results = {2: {}, 4: {}, 6: {}, 8: {}}
    for i in range(len(list(results.keys()))):
        print("Running with %d threads..." % list(results.keys())[i])
        # replace number of threads
        if i != 0:
            f = open("db_operator.c", "r")
            src = f.read()
            src = src.replace("query_chunk_size = %d" % list(results.keys())[i - 1], "query_chunk_size = %d" % list(results.keys())[i])
            f = open("db_operator.c", "w")
            f.write(src)
            f.close()

        # run scans
        for num_run in range(max_num_scans):
            print("Testing %d scans..." % (num_run + 1))

            # run and time each script
            script_time = run_script(scripts[num_run])
            results[list(results.keys())[i]][num_run + 1] = script_time

        # shutdown server and re make
        tmp_script = "shutdown\n"
        run_script(tmp_script)
        os.system("make > /dev/null")

        # run server
        print("Running server...")
        os.system("./server > server.out &")
        time.sleep(1)

    return results


def generate_selection_vals(num_runs):
    selection_vals = []
    for i in range(num_runs):
        selectivity = (i + 1) * 1. / num_runs;

        min_val = random.randint(0, 1000000 - selectivity * 1000000)
        max_val = min_val + int(1000000 * selectivity)

        selection_vals.append((min_val, max_val))

    return selection_vals


def generation_selection_line(num_run, min_val, max_val):
    return "s{0}=select(db1.tbl3.col2,{1},{2})\n".format(num_run, min_val, max_val)



def run_index_selection_large(selection_vals, index_type = ""):
    print("Running with index type: %s..." % index_type)
    results = []
    num_runs = 10


    # compile setup lines
    setup = create_db
    lines = [
        'create(tbl,"tbl3",db1,4)',
        'create(col,"col1",db1.tbl3)',
        'create(col,"col2",db1.tbl3)',
        'create(col,"col3",db1.tbl3)',
        'create(col,"col4",db1.tbl3)',
        'create(idx,db1.tbl3.col2,{},unclustered)'.format(index_type),
    ]

    for num_test in range(num_runs):
        # build
        os.system("make clean > /dev/null")
        os.system("make > /dev/null")

        test_lines = lines         
        if not index_type:
            test_lines.pop(5)

        print("Running with %d rows..." % ((num_test + 1) * 500000))
        test_lines.append('load("/home/ec2-user/project/project_tests_1M/datatemp.csv",%d)' % ((num_test + 1) * 5000))

        setup += "\n".join(test_lines) + "\n"

        # run server
        print("Running server...")
        os.system("./server > server.out &")
        time.sleep(1)

        # run setup
        print("Running setup...")
        run_script(setup)
        
        total_time = 0
        for i in range(100):
            # run selection
            ratio = ((num_test + 1) * 500000) / 1000000
            script = generation_selection_line(i, selection_vals[i][0] * ratio, selection_vals[i][1] * ratio)

            # run and time each script
            script_time = run_script(script)

            total_time += script_time

        avg_time = total_time / 100
        results.append(avg_time)
        os.system("sudo pkill -f 'server'")

    return results


def run_index_selection(selection_vals, index_type = ""):
    print("Running with index type: %s..." % index_type)
    results = []
    num_runs = 100

    # build
    os.system("make clean > /dev/null")
    os.system("make > /dev/null")
    # compile setup lines
    setup = create_db
    lines = [
        'create(tbl,"tbl3",db1,4)',
        'create(col,"col1",db1.tbl3)',
        'create(col,"col2",db1.tbl3)',
        'create(col,"col3",db1.tbl3)',
        'create(col,"col4",db1.tbl3)',
        'create(idx,db1.tbl3.col2,{},unclustered)'.format(index_type),
        'load("/home/ec2-user/project/project_tests_1M/data3.csv")',
    ]

    if not index_type:
        lines.pop(5)

    setup += "\n".join(lines) + "\n"

    # run server
    print("Running server...")
    os.system("./server > server.out &")
    time.sleep(1)

    # run setup
    print("Running setup...")
    run_script(setup)

    # run selections
    for i in range(num_runs):
        selectivity = (i + 1) * 1. / num_runs;

        script = generation_selection_line(i, selection_vals[i][0], selection_vals[i][1])

        # run and time each script
        script_time = run_script(script)

        results.append(script_time)

    return results


def run_btree_node_size_expirement(selection_vals):
    results = []
    print("running for 5 row sizes")
    for num_rows in [200000, 400000, 600000, 800000, 1000000]:
        num_rows = num_rows
        print("Running %d rows..." % num_rows)
        num_runs = 100

        # build
        os.system("make clean > /dev/null")
        os.system("make > /dev/null")
        # compile setup lines
        setup = create_db
        lines = [
            'create(tbl,"tbl3",db1,4)',
            'create(col,"col1",db1.tbl3)',
            'create(col,"col2",db1.tbl3)',
            'create(col,"col3",db1.tbl3)',
            'create(col,"col4",db1.tbl3)',
            'create(idx,db1.tbl3.col4,btree,unclustered)',
            'load("/home/ec2-user/project/project_tests_1M/data3.csv",{})'.format(num_rows),
        ]

        setup += "\n".join(lines) + "\n"

        # run server
        print("Running server...")
        os.system("./server > server.out &")
        time.sleep(1)

        start = time.time()
        # run setup
        print("Running setup...")
        run_script(setup)

        start_2 = time.time()

        ratio = num_rows / 1000000
        for i in range(num_runs):
            selectivity = (i + 1) * 1. / num_runs;

            script = generation_selection_line(i, selection_vals[i][0] * ratio, selection_vals[i][1] * ratio)

            # run and time each script
            script_time = run_script(script)

        end = time.time()
        
        results.append((int((end - start) * 1000000), int((end - start_2) * 1000000)))

        # kill server
        os.system("pkill -f './server'")

    return results


def generate_joins_lines(num_test):
    # get join sizes
    mid = int(1000000 / 2)
    left_position_end = mid + (num_test * 2500)
    left_position_begin = mid - (num_test * 5000)
    right_position_begin = mid - (num_test * 2500)
    right_position_end = mid + (num_test * 5000)

    join_size = left_position_end - left_position_begin

    # generate setup lines
    setup_lines = [
        "s1{}=select(db1.tbl3.col1,{},{})".format(num_test, left_position_begin, left_position_end),
        "s2{}=select(db1.tbl4.col1,{},{})".format(num_test, right_position_begin, right_position_end),   
        "f1{0}=fetch(db1.tbl3.col1,s1{0})".format(num_test),
        "f2{0}=fetch(db1.tbl4.col1,s2{0})".format(num_test),
    ]

    # generate nested join command
    nested_join = "t1n{0},t2n{0}=join(f1{0},s1{0},f2{0},s2{0},nested-loop)\nshutdown".format(num_test)
    # generate grace join command
    hash_join = "t1h{0},t2h{0}=join(f1{0},s1{0},f2{0},s2{0},hash)\nshutdown".format(num_test)
    setup = "\n".join(setup_lines) + "\n"
    return setup + nested_join, setup +  hash_join, join_size


def run_nested_vs_hash_expirement():
    # compile setup lines
    setup = create_db
    lines = [
        'create(tbl,"tbl3",db1,4)',
        'create(col,"col1",db1.tbl3)',
        'create(col,"col2",db1.tbl3)',
        'create(col,"col3",db1.tbl3)',
        'create(col,"col4",db1.tbl3)',
        'create(tbl,"tbl4",db1,4)',
        'create(col,"col1",db1.tbl4)',
        'create(col,"col2",db1.tbl4)',
        'create(col,"col3",db1.tbl4)',
        'create(col,"col4",db1.tbl4)',
        'load("/home/ec2-user/project/project_tests_1M/data3.csv")',
        'load("/home/ec2-user/project/project_tests_1M/data4.csv")',
        'shutdown'
    ]
    setup += "\n".join(lines) + "\n"

    max_num_joins = 10

    # build
    os.system("make clean > /dev/null")
    os.system("make > /dev/null")

    # run server
    print("Running server...")
    os.system("./server > server.out &")
    time.sleep(1)

    # run setup
    print("Running setup...")
    run_script(setup)

    # generate scripts
    print("Generating scripts...")
    scripts = []
    for i in range(max_num_joins):
        nested_join, hash_join, join_size = generate_joins_lines(i + 1)
        scripts.append((nested_join, hash_join, join_size))

    results = []
    # run joins
    for num_run in range(max_num_joins):
        print("Testing %d join..." % (num_run + 1))

        # run setup
        os.system("./server > server.out &")
        time.sleep(1)

        # print("running nested...")
        # run nested
        # nested_time = run_script(scripts[num_run][0])
        # print("nested time %d" % nested_time)
        # time.sleep(.1)
        # os.system("./server > server.out &")
        # time.sleep(1)

        # run hash
        hash_time = run_script(scripts[num_run][1])
        time.sleep(.1)

        run_script("shutdown\n")
        # add to results
        results.append((scripts[num_run][2], hash_time))

    return results


def run_expirements(expirement_num = 1):
    print("Building...")
    os.system("sudo make clean > /dev/null")
    os.system("sudo make > /dev/null")
    
    # different number of threads
    if expirement_num == 1:
        results = run_shared_scans_expirement()

        f = open("../expirement_results/shared_scans_chunks_results.csv", "w")
        f.write("num_scans,c.2,c.4,c.6,c.8\n")
        for i in range(100):
            f.write("{},".format(i + 1))
            for key in [2,4,6,8]:
                f.write("{0}".format(results[key][i + 1]))
                if key != 8:
                    f.write(",")
            f.write("\n")
        f.close()

    # shared vs indidividual scans
    if expirement_num == 2:
        results = run_shared_individual_expirement()

        # dump results
        f = open("../expirement_results/shared_scans_threads_results.csv", "w")
        f.write("num_scans,time\n")
        for num_scans, time in results["shared"].items():
            f.write("{0},{1}\n".format(num_scans, time))
        f.close()

        f = open("../expirement_results/individual_scans_results.csv", "w")
        f.write("num_scans,time\n")
        for num_scans, time in results["individual"].items():
            f.write("{0},{1}\n".format(num_scans, time))
        f.close()
    
    # different btree node sizes
    elif expirement_num == 3:
        print("Generating selection vals...")
        selection_vals = generate_selection_vals(100)

        all_results = {}
        # run for node size one page
        print("Running for node size 1...")
        all_results[1] = run_btree_node_size_expirement(selection_vals)
        
        # run for node size 4
        print("Running for node size 2...")
        # replace FANOUT and LEAF SIZE in include/cs165_api.h
        f = open("include/cs165_api.h", "r")
        src = f.read()
        src = src.replace("FANOUT 340", "FANOUT 170")
        src = src.replace("LEAF_SIZE 508", "LEAF_SIZE 254")
        f = open("include/cs165_api.h", "w")
        f.write(src)
        f.close()

        all_results[2] = run_btree_node_size_expirement(selection_vals)
        

        # run for node size 8
        print("Running for node size 3...")
        # replace FANOUT and LEAF SIZE in include/cs165_api.h
        f = open("include/cs165_api.h", "r")
        src = f.read()
        src = src.replace("FANOUT 170", "FANOUT 85")
        src = src.replace("LEAF_SIZE 254", "LEAF_SIZE 127")
        f = open("include/cs165_api.h", "w")
        f.write(src)
        f.close()
        
        all_results[3] = run_btree_node_size_expirement(selection_vals)


        # run for node size .1
        print("Running for node size 4...")
        # replace FANOUT and LEAF SIZE in include/cs165_api.h
        f = open("include/cs165_api.h", "r")
        src = f.read()
        src = src.replace("FANOUT 85", "FANOUT 42")
        src = src.replace("LEAF_SIZE 127", "LEAF_SIZE 64")
        f = open("include/cs165_api.h", "w")
        f.write(src)
        f.close()
        
        all_results[4] = run_btree_node_size_expirement(selection_vals)

        # run for node size .1
        print("Running for node size 5...")
        # replace FANOUT and LEAF SIZE in include/cs165_api.h
        f = open("include/cs165_api.h", "r")
        src = f.read()
        src = src.replace("FANOUT 42", "FANOUT 21")
        src = src.replace("LEAF_SIZE 64", "LEAF_SIZE 32")
        f = open("include/cs165_api.h", "w")
        f.write(src)
        f.close()
        
        all_results[5] = run_btree_node_size_expirement(selection_vals)


        # dump results
        f = open("../expirement_results/btree_node_size_results.csv", "w")
        f.write("num_rows,one,two,three,four,five\n")
        f2 = open("../expirement_results/btree_node_size_results_2.csv", "w")
        f2.write("num_rows,one,two,three,four,five\n")

        for i in range(5):
            # num_rows = 1000000
            num_rows = [100000, 200000, 300000, 400000, 500000, 600000, 700000, 800000, 900000, 1000000][i]
            f.write("%d," % num_rows)
            f2.write("%d," % num_rows)
            for key in [1,2,3,4,5]:
                f.write("{0}".format(all_results[key][i][0]))
                f2.write("{0}".format(all_results[key][i][1]))
                if key != 8:
                    f.write(",")
                    f2.write(",")
            f.write("\n")
            f2.write("\n")
        f.close()
        f2.close()

    # btree vs sorted vs none
    elif expirement_num == 4:
        print("Generating selection vals...")
        selection_vals = generate_selection_vals(100)

        all_results = {}
        for index_type in ["btree", "sorted", ""]:
            label = index_type
            if not label:
                label = "none"

            all_results[label] = run_index_selection(selection_vals, index_type) 

        # dump results
        f = open("../expirement_results/index_comparison.csv", "w")
        f.write("selectivity,btree,sorted,none\n")
        for i in range(100):
            f.write("{0},{1},{2},{3}\n".format(
                (i + 1) * 1. / 100,
                all_results["btree"][i],
                all_results["sorted"][i],
                all_results["none"][i]  
            ))
        f.close()

    # nested loop vs hash join
    elif expirement_num == 5:
        print("Running grace vs nested loop join...")
        result = run_nested_vs_hash_expirement()

        f = open("../expirement_results/nested_vs_hash.csv", "w")
        f.write("join.size, nested, hash\n")
        for i in range(100):
            f.write("{0},{1},{2}\n".format(result[i][0], result[i][1], result[i][2]))
        f.close()

    elif expirement_num == 6:
        all_results = {}
        # run for node size one page
        print("Running for 256...")
        all_results[256] = run_nested_vs_hash_expirement()
        
        # run for node size pages
        print("Running for 128...")
        # replace FANOUT and LEAF SIZE in include/cs165_api.h
        f = open("db_operator.c", "r")
        src = f.read()
        src = src.replace("num_partitions = 256", "num_partitions = 128")
        f = open("db_operator.c", "w")
        f.write(src)
        f.close()

        all_results[128] = run_nested_vs_hash_expirement()
        print(all_results[128])
        # run for node size pages
        print("Running for 64...")
        # replace FANOUT and LEAF SIZE in include/cs165_api.h
        f = open("db_operator.c", "r")
        src = f.read()
        src = src.replace("num_partitions = 128", "num_partitions = 64")
        f = open("db_operator.c", "w")
        f.write(src)
        f.close()

        all_results[64] = run_nested_vs_hash_expirement()
        print(all_results[64])
        
        # run for node size pages
        print("Running for 32...")
        # replace FANOUT and LEAF SIZE in include/cs165_api.h
        f = open("db_operator.c", "r")
        src = f.read()
        src = src.replace("num_partitions = 64", "num_partitions = 32")
        f = open("db_operator.c", "w")
        f.write(src)
        f.close()

        all_results[32] = run_nested_vs_hash_expirement()
        print(all_results[32])
        
        # run for node size pages
        print("Running for 16...")
        # replace FANOUT and LEAF SIZE in include/cs165_api.h
        f = open("db_operator.c", "r")
        src = f.read()
        src = src.replace("num_partitions = 32", "num_partitions = 16")
        f = open("db_operator.c", "w")
        f.write(src)
        f.close()

        all_results[16] = run_nested_vs_hash_expirement()
        print(all_results[16])

        f = open("../expirement_results/partition_sizes.csv", "w")
        f.write("join.size,p.256,p.128,p.64,p.32,p.16\n")
        for i in range(10):
            f.write("%d," % all_results[256][i][0])
            for key in [256,128,64,32,16]:
                f.write("%d" % all_results[key][i][1])

                if key != 16:
                    f.write(",")
            f.write("\n")
        f.close()


if __name__ == "__main__":
    num_expirement = 1

    # parse args
    i = 1
    while i < len(sys.argv):
        arg_name = sys.argv[i]
        i += 1

        if arg_name == "--expirement":
            num_expirement = int(sys.argv[i])
            i += 1


    run_expirements(num_expirement)
