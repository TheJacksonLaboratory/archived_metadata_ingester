# This will pull a unique list of userids from the cost center dump json, removing any id with a hyphen.
# It will print each user's groups, minus common groups we're not interested in, such as 'jaxuser' or 'rshiny-users'.
# If the user doesn;t belong to a group besides those common groups, their userid is not printed.
#
# Note: this script is not currently used, as we've opted to err on the side of conservative by only associating PIs
# with their lab name, as opposed to any dbgap, sdata, or other special project groups they might belong to. Should we
# decide to expand that in the future, this could be of assistance.
for user in $(grep userid cost_center_dump.txt | cut -f2 -d: | tr -d '",' | grep -v '-' | sort | uniq) ; do groups $user 2> /dev/null | sed -e 's/jaxuser//' -e 's/rstudio-users//' -e 's/helix-batch2//' -e 's/rshiny-users//' | grep -E "*:\s*[a-z]" ; done

