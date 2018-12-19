source activate fs
SESSION=RPYC 
tmux new -s ${SESSION} -d

tmux split-window -v -p 50 -t $SESSION:0.0
tmux split-window -h -p 66 -t $SESSION:0.0
tmux split-window -h -p 50 -t $SESSION:0.0
tmux split-window -h -p 66 -t $SESSION:0.3
tmux split-window -h -p 50 -t $SESSION:0.3

tmux send -t $SESSION:0.0 "conda activate fs; python -u namenode.py 20000 | tee namenode.log" Enter
for i in {1..5}
do
    tmux send -t $SESSION:0.$i "conda activate fs; python datanode.py $((i + 20000)) " Enter
done

tmux a -t $SESSION

tmux kill-session -t $SESSION