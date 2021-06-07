#!/bin/bash
version="latest"
createvol=0


# ping storage.jd.com -c 1 -i 1 -w 1 &> /dev/null
# if [[ $? -eq 0 ]];then
#     domain=storage.360buyimg.com
# else
#     domain=storage.jd.local
# fi

function help(){
    echo "Usage: install.sh -r | --role [datanode | metanode | master | objectnode | console | monitor | client | all | createvol ]"
    exit
}

function createTestVol(){
    createtestvol=$(ansible -i iplist master -m debug -a "msg={% set pk = groups['master'] %}{% for host in pk %}{% if loop.first %}http://{{ host }}:{{ master_listen}}/admin/createVol?name={{ client_volName }}&capacity={{ client_SizeGB }}&owner={{ client_owner }}{% endif %}{% endfor %}" -o |awk -F'"' '{if(NR==1 ){print $(NF-1)}}')
    echo "$createtestvol"
    seq 1 300 | while read i;do
        /bin/curl -s $createtestvol |grep successfully && break
        sleep 1
        echo -n "."
    done

}


function install(){
    for role in $role;do
        # url="http://$domain/chubaofsrpm/${version}/cfs-${role}-${version}-el7.x86_64.rpm "
        if [[ $createvol -eq 1 ]] && [[ "$role" == "client" ]] ;then
            createTestVol
        fi

        # url="http://$domain/chubaofsrpm/${version}/cfs-monitor-x86_64.tar.gz "
        # cd src
        # wget -N $url
        # if [ $? -ne 0 ];then
        #     echo "$url : Url not found."
        #     exit 
        # fi
        # cd -
        ansible-playbook -i iplist install_cfs.yml -e "cfsrole=${role} version=${version}"
    
    done
}


ARGS=( "$@" )
for opt in ${ARGS[*]} ; do
    case "$opt" in
        -h|--help)
            help
            ;;
        -v|--version)
            shift
            version=$1
            shift
            ;;
        -r|--role)
            shift
            roleargs=$1
            for op in ${roleargs[*]} ; do
                case "$op" in
                    master)
                        role="master"
                        ;;
                    datanode)
                        role="datanode"
                        ;;
                    metanode)
                        role="metanode"
                        ;;
                    objectnode)
                        role="objectnode"
                        ;;
                    console)
                        role="console"
                        ;;
                    monitor)
                        role="monitor"
                        ;;
                    client)
                        role="client"
                        ;;
                    createvol)
                        createTestVol
                        ;;
                    all)
                        role="master metanode datanode objectnode monitor console client"
                        createvol=1
                        ;;
                    *)
                        ;;
                esac
            done

            shift
            ;;
        *)
            ;;
    esac
done

if [[ "X$role" == "X" ]];then
    help
fi

install $version $role

