#########################################################################
# Script to send review requests:                                       #
# Use the parameter -p to set the reviwer(s)                            #
# Use the parameter -r to send a new diff to a existent review request  #
#########################################################################

COMMAND="rbt post "
declare REVIWER=""
declare REVIEW_NUM=""
declare AUTOMATIC_PREVIEW=""

while getopts ":p:r:" opt; do
  case $opt in
    p)
      REVIWER="--target-people=$OPTARG"
      ;;
    r)
      REVIEW_NUM="-r=$OPTARG"
      ;;
    -pu)
      AUTOMATIC_PREVIEW="-p"
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
  esac
done

$COMMAND $REVIWER $REVIEW_NUM $AUTOMATIC_PREVIEW
