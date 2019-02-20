#!/bin/bash

##################跑之前要根据文件prefetch.h的具体值设置下面两个变量########################
oldscalarstatesize=30
oldseq_dis=30
oldpdis=256
oldsimdstatesize=5

########################
app="NPO"
#app="PIPELINE"
#app="BTS"

#keep the same with REPEAT_PROBE
repeat=2

statestart=1
stateend=15
disstart=0
disend=640

numaid=0

function run_a_cycle(){
#	r_size_set=(64000000 32000000 16000000 8000000 4000000)
	r_size_set=(64000000 32000000)
	#s_size_set=("6.4e+08" "3.2e+08" "1.6e+08")
	s_size_set=("6.4e+08" "3.2e+08")
	r_skew_set=(0 0.5 1)
	s_skew_set=(0 0.5 1)
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
#						echo "s_size: ${s_size_set[j]}"
						echo "r_skew: ${r_skew_set[k]}"
						echo "s_skew: ${s_skew_set[l]}"
						#########################################
						echo "ARGS dis: $2 statesize: $3 thread_num: ${thread_nums[t]} r_size: ${r_size_set[i]} s_size: ${s_size_set[j]} r_skew: ${r_skew_set[k]} s_skew: ${s_skew_set[l]}" >> $1
						time HUGETLB_MORECORE=yes LD_PRELOAD=$HUGEPAGE_HOME/libhugetlbfs.so numactl -N $numaid -m $numaid  ./mchashjoins -a $app -n ${thread_nums[t]} --r-file=r_skew=${r_skew_set[k]}_size=${r_size_set[i]} --s-file=s_skew=${s_skew_set[l]}_size=${s_size_set[j]} >> $1
					done;
				done;
			done;
		done;
		echo "!!!" >> $1
	done;
}


echo "using ./test.sh 1 to change oldsimdstatesize and oldpdis....\n"
echo "using ./test.sh 2 to change scalarstatesize and seq_dis....\n"

if [ "$1" == '1' ] 
then

        for((pdis=$disstart;pdis<=$disend;pdis+=64)) do
                #############修改prefetch.h文件##################
                sed -i "s/define\ PDIS\ ${oldpdis}/define\ PDIS\ ${pdis}/g" prefetch.h
                for((simdstatesize=$statestart;simdstatesize<=$stateend;simdstatesize++)) do
                        output_file=results/${app}_pdis_${pdis}_simdstatesize_${simdstatesize}.txt
                        echo $output_file
                        #############修改prefetch.h文件##############
                        sed -i "s/define\ SIMDStateSize\ ${oldsimdstatesize}/define\ SIMDStateSize\ ${simdstatesize}/g" prefetch.h
			echo "pdis_${pdis}_simdstatesize_${simdstatesize}" >> chg_prefetch_h.log
                        cat prefetch.h >> chg_prefetch_h.log
                        cd ..
                        ##########################
                        make
                        cd src
                        echo "start to run..."
                        run_a_cycle $output_file ${pdis} ${simdstatesize}
                        echo "end run..."
                        results_file=results/${app}_results_pdis_${pdis}_simdstatesize_${simdstatesize}.csv
                        echo "start to parse..."
                        echo $output_file
                        #############解析结果的脚本，第一个参数是源文件，第二个参数是结果文件，第三个参数是你repeat的次数################
                        python test_results_parser.py $output_file $results_file $repeat
			rm ${output_file}
                        echo "end parse..."
                        oldsimdstatesize=$simdstatesize
                done;
                oldpdis=${pdis}
        done;
fi

if [ "$1" == '2' ] 
then

	for((scalarstatesize=$statestart;scalarstatesize<=$stateend;scalarstatesize++)) do
		#############修改prefetch.h文件##################
		sed -i "s/define\ ScalarStateSize\ ${oldscalarstatesize}/define\ ScalarStateSize\ ${scalarstatesize}/g" prefetch.h
		for((seq_dis=$disstart;seq_dis<=$disend;seq_dis++)) do
			output_file=results/${app}_scalarstatesize_${scalarstatesize}_seqdis_${seq_dis}.txt
			echo $output_file
			#############修改prefetch.h文件##############
			sed -i "s/define\ SEQ_DIS\ ${oldseq_dis}/define\ SEQ_DIS\ ${seq_dis}/g" prefetch.h
			echo "scalarstatesize_${scalarstatesize}_seqdis_${seq_dis}" >> chg_prefetch_h.log
			cat prefetch.h >> chg_prefetch_h.log
			cd ..
			##########################
			make
			cd src
			echo "start to run..."
			run_a_cycle $output_file ${seq_dis} ${scalarstatesize}
			echo "end run..."
			results_file=results/results_${app}_scalarstatesize_${scalarstatesize}_seqdis_${seq_dis}.csv
			echo "start to parse..."
			echo $output_file
			#############解析结果的脚本，第一个参数是源文件，第二个参数是结果文件，第三个参数是你repeat的次数################
			python test_results_parser.py $output_file $results_file $repeat
			rm ${output_file}
			echo "end parse..."
			oldseq_dis=${seq_dis}
		done;
		oldscalarstatesize=${scalarstatesize}
	done;
fi
