#!/bin/bash

#keep the same with REPEAT_PROBE
repeat=2

numa_nid=0
numa_mid=0

function run_a_cycle(){
#	r_size_set=(64000000 32000000 16000000 8000000 4000000)
	r_size_set=("64M")
	#s_size_set=("6.4e+08" "3.2e+08" "1.6e+08")
	s_size_set=("500M")
	r_skew_set=(0.5)
	s_skew_set=(0.5)
	#thread_nums=(1 4 8 16 24 32)
	thread_nums=(1)
	#################NOTE: 请不要注释掉以下循环，修改参数请在上面修改###########################
	for ((t=0;t<${#thread_nums[@]};t++)) do
		for ((i=0;i<${#r_size_set[@]};i++)) do
			for ((j=0;j<${#s_size_set[@]};j++)) do
				for ((k=0;k<${#r_skew_set[@]};k++)) do
					for ((l=0;l<${#s_skew_set[@]};l++)) do
						echo "thread_num: ${thread_nums[t]}"
						echo "r_size: ${r_size_set[i]}"
						echo "s_size: ${s_size_set[j]}"
						echo "r_skew: ${r_skew_set[k]}"
						echo "s_skew: ${s_skew_set[l]}"
						#########################################
						echo "ARGS dis: $2 SIMDstatesize: $3 scalarstatesize: $4 thread_num: ${thread_nums[t]} r_size: ${r_size_set[i]} s_size: ${s_size_set[j]} r_skew: ${r_skew_set[k]} s_skew: ${s_skew_set[l]}" >> $1
						time HUGETLB_MORECORE=yes LD_PRELOAD=$HUGEPAGE_HOME/libhugetlbfs.so numactl -N $numa_nid -m $numa_mid  ./mchashjoins -a $app -n ${thread_nums[t]} --r-file=r_skew=${r_skew_set[k]}_size=${r_size_set[i]} --s-file=s_skew=${s_skew_set[l]}_size=${s_size_set[j]}_max=${r_size_set[i]}  >> $1
					done;
				done;
			done;
		done;
		echo "!!!" >> $1
	done;
}

function expr_group_size() {
	# paramter 1: state size
	# range=1~40, +4 scalarstatesize
	# range=1~20, +1 simdstatesize=5
	sstatestart=1
	sstateend=50
	statestart=1
	stateend=30
	# paramter 2: sequential prefetch distance
	disstart=640
	disend=640
	# paramter 3:application name
	app="NPO"
	#app="PIPELINE"
	#app="BTS"
	dir_name="results_group"_$(date +%F-%T)
	mkdir $dir_name
	for((pdis=$disstart;pdis<=$disend;pdis+=64)) do
		#############修改prefetch.h文件##################
		sed -i "/#define\ PDIS/c\#define\ PDIS\ ${pdis}" prefetch.h
		for((scalarstatesize=$sstatestart,simdstatesize=$statestart;scalarstatesize<=$sstateend;scalarstatesize+=2,simdstatesize+=1)) do
		        output_file=${dir_name}/${app}_pdis_${pdis}_simdstatesize_${simdstatesize}_scalarstatesize_${scalarstatesize}.txt
		        echo $output_file
		        #############修改prefetch.h文件##############
		        sed -i "/#define\ SIMDStateSize/c\#define\ SIMDStateSize\ ${simdstatesize}" prefetch.h
		        sed -i "/#define\ ScalarStateSize/c\#define\ ScalarStateSize\ ${scalarstatesize}" prefetch.h
			echo "pdis_${pdis}_simdstatesize_${simdstatesize}_scalarstatesize_${scalarstatesize}" >> chg_prefetch_h.log
		        cat prefetch.h >> chg_prefetch_h.log
		        cd ..
		        ##########################
		        make
		        cd src
		        echo "start to run..."
		        run_a_cycle $output_file ${pdis} ${simdstatesize} ${scalarstatesize}
		        echo "end run..."
		        results_file=${dir_name}/${app}_results_pdis_${pdis}_simdstatesize_${simdstatesize}_scalarstatesize_${scalarstatesize}.csv
		        echo "start to parse..."
		        echo $output_file
		        #############解析结果的脚本，第一个参数是源文件，第二个参数是结果文件，第三个参数是你repeat的次数################
		        python test_results_parser.py $output_file $results_file $repeat
			rm ${output_file}
		        echo "end parse..."
		done;
	done;
}
function expr_pdis() {
	# paramter 1: state size
	# range=1~40, +4 scalarstatesize
	# range=1~20, +1 simdstatesize=5
	sstatestart=30
	sstateend=30
	statestart=5
	stateend=5
	# paramter 2: sequential prefetch distance
	disstart=0
	disend=1280
	# paramter 3:application name
	app="NPO"
	#app="PIPELINE"
	#app="BTS"
	dir_name="results_pdis"_$(date +%F-%T)
	mkdir $dir_name
	for((pdis=$disstart;pdis<=$disend;pdis+=64)) do
		#############修改prefetch.h文件##################
		sed -i "/#define\ PDIS/c\#define\ PDIS\ ${pdis}" prefetch.h
		for((scalarstatesize=$sstatestart,simdstatesize=$statestart;scalarstatesize<=$sstateend;scalarstatesize+=2,simdstatesize+=1)) do
		        output_file=${dir_name}/${app}_pdis_${pdis}_simdstatesize_${simdstatesize}_scalarstatesize_${scalarstatesize}.txt
		        echo $output_file
		        #############修改prefetch.h文件##############
		        sed -i "/#define\ SIMDStateSize/c\#define\ SIMDStateSize\ ${simdstatesize}" prefetch.h
		        sed -i "/#define\ ScalarStateSize/c\#define\ ScalarStateSize\ ${scalarstatesize}" prefetch.h
			echo "pdis_${pdis}_simdstatesize_${simdstatesize}_scalarstatesize_${scalarstatesize}" >> chg_prefetch_h.log
		        cat prefetch.h >> chg_prefetch_h.log
		        cd ..
		        ##########################
		        make
		        cd src
		        echo "start to run..."
		        run_a_cycle $output_file ${pdis} ${simdstatesize} ${scalarstatesize}
		        echo "end run..."
		        results_file=${dir_name}/${app}_results_pdis_${pdis}_simdstatesize_${simdstatesize}_scalarstatesize_${scalarstatesize}.csv
		        echo "start to parse..."
		        echo $output_file
		        #############解析结果的脚本，第一个参数是源文件，第二个参数是结果文件，第三个参数是你repeat的次数################
		        python test_results_parser.py $output_file $results_file $repeat
			rm ${output_file}
		        echo "end parse..."
		done;
	done;
}
	

echo "input expr name : 
G: group size
D: prefetch distance
G+D: G+D
------------------"
read expr_name

if [[ ${expr_name} == 'D' ]]; then 
	expr_pdis
elif [[ ${expr_name} == 'G' ]]; then	
	expr_group_size
elif [[ ${expr_name} == 'G+D' ]]; then
	expr_group_size
	expr_pdis
else 
	echo -n ${expr_name}
	echo " not found"
fi


