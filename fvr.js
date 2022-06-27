var platforms=["mobile","other"];
var states=["fl","mi","not_usa","ny","other","tx","wa"];
var refs=["direct","external","other","search","social"];
var weekdays=[0,1,2,3,4,5,6];
var hours=[10,11,12,13,14,15,16,17,18,19,2,20,3,4,5,6,7,8,9,other];
if (platform=='mobile'){
	if (state=='fl'){
		if (ref=='direct'){
			if (weekday==0){
				if ([12,14,16,18,5,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([10,12,13,17,20,4,7].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([11,14,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([10,12,15,16,17,18,19,5].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([13,20,6].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([10,14,15,16,17,20,5,6,7,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([10,12].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='external'){
			if (weekday==0){
				if ([14,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([14,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if (hour==20){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='other'){
			if (weekday==2){
				if (hour==13){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([11,6].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([2,3].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='search'){
			if (weekday==0){
				if ([16,7,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([10,17].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([16,7].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([14,16,18,6].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if (hour==10){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if (hour==6){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='social'){
			if (weekday==0){
				if ([11,17,18,19,4,7].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([13,17,2,4,5,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([12,17,20,4,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([13,3,4].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([10,14,5].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([11,14,4,6,7].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([12,19,7,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
	}
	else if (state=='mi'){
		if (ref=='direct'){
			if (weekday==0){
				if ([11,13,15,6].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([14,16,17,4].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([11,16,4,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if (hour==15){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([10,11,2].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([14,18,20,3].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([12,3,5,6].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='external'){
			if (weekday==4){
				if (hour==10){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([13,8].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='other'){
			if (weekday==0){
				if ([12,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if (hour==15){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if (hour==13){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([2,3].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if (hour==6){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([17,8].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='search'){
			if (weekday==0){
				if (hour==11){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if (hour==14){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if (hour==18){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([18,19,20].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if (hour==10){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([18,4,6].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if (hour==14){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='social'){
			if (weekday==0){
				if ([17,18,20].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([14,16,17,19,4,6,7,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([10,2].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([11,5].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([10,13,6,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([16,6,8,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([10,9].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
	}
	else if (state=='not_usa'){
		if (ref=='external'){
			if (weekday==1){
				if ([18,5,7].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([3,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if (hour==4){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='other'){
			if (weekday==0){
				if ([15,16].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if (hour==3){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if (hour==17){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if (hour==12){
					TARGET=TRUE;
				}
			}
		}
	}
	else if (state=='ny'){
		if (ref=='direct'){
			if (weekday==0){
				if ([10,11,14,15,19,2,3,5,6,7,8,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([12,13,16,4,5,6,8,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([11,12,13,15,16,17,18,4,5,7].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([13,14,15,16,2,3,4,5,6,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([11,13,14,15,3,4,5,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([11,12,13,14,20,6,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([10,11,17,18,19,2,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='external'){
			if (weekday==0){
				if ([13,20,4,6].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([16,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if (hour==5){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([13,17,18].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([15,18].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([10,5,7].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='other'){
			if (weekday==0){
				if (hour==12){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([4,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if (hour==7){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([4,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='search'){
			if (weekday==0){
				if ([13,16,18,4,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([14,15,16,18,19,2,3,5].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([10,13,15,16,17,18,3,4,7,8,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([11,12,14,16,18,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([12,16,17,18,19,20,3,4,7,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([11,14,15,16,5,7].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([10,13,15,16,17,18,19,20,3,7,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='social'){
			if (weekday==0){
				if ([10,11,14,15,16,19,20,7,8,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([15,17,18,6,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([12,19,2,20,3,5,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([14,15,17,2,20,3,5,8,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([12,14,16,20,3,5,6,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([11,19,20,4,5,6,7,8,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([10,13,14,15,16,20,6,7,8].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
	}
	else if (state=='other'){
		if (ref=='direct'){
			if (weekday==0){
				if ([13,14,5].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([10,11,18,19,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([11,12,17,20].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([10,11,16,17,6,7].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([11,12,13,14,15,16,5,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([4,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([2,4,9].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='external'){
			if (weekday==0){
				if ([13,19,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([12,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([19,5].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if (hour==11){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([16,18,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([13,15,6].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='other'){
			if (weekday==0){
				if ([12,4,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([15,20].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if (hour==13){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([11,14].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([2,3].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([10,6].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([17,3,6,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='search'){
			if (weekday==0){
				if (hour==18){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([13,5,6].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if (hour==13){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([5,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if (hour==9){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([11,20].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([11,2,20,5].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='social'){
			if (weekday==0){
				if (hour==3){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([11,20].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if (hour==20){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([20,6].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([15,2,20].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
	}
	else if (state=='tx'){
		if (ref=='direct'){
			if (weekday==0){
				if ([10,13,15,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([11,16,20].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([10,11,12,15,19,20,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([10,13,15,18,3,4,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([10,11,4].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([14,15,2,3,6,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([12,15,16,20,3,5,8].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='external'){
			if (weekday==0){
				if ([13,2,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([16,2].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([15,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if (hour==3){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='other'){
			if (weekday==2){
				if (hour==6){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='search'){
			if (weekday==0){
				if ([10,13,16,17].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if (hour==14){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([12,14,16,3,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([19,7].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if (hour==3){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([14,4,8].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='social'){
			if (weekday==0){
				if ([12,18,4,6,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([13,15,20,3].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([11,14,4,5,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([10,13,15,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([10,4,5,6].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([15,17,19,20,6].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([13,20,6,7].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
	}
	else if (state=='wa'){
		if (ref=='direct'){
			if (weekday==0){
				if ([11,12,18,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([10,13,14,16,19].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([18,5,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([10,16].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([10,12,13,15,19,4,5,7,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([10,16,18,5].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([14,5,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='external'){
			if (weekday==4){
				if (hour==6){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if (hour==13){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='other'){
			if (weekday==0){
				if (hour==12){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if (hour==14){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([17,18].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='search'){
			if (weekday==0){
				if ([14,15,3,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([16,19,4,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if (hour==8){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if (hour==12){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if (hour==14){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([12,2].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='social'){
			if (weekday==0){
				if ([10,11,12,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([18,6,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([16,17].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([15,19,5,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([4,6,7,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([14,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([10,13,3,4,5,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
	}
}
else if (platform=='other'){
	if (state=='fl'){
		if (ref=='direct'){
			if (weekday==0){
				if ([10,11,12,13,14,15,16,18,20,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([12,13,15,17,2,20,4,5,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([10,12,13,14,15,16,18,19,2,6,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([10,11,12,13,14,16,20,3,4,5,6,7,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([11,12,13,14,17,18,20,3,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([10,12,13,15,16,17,3,4,5,7,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([11,12,13,17,2,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='external'){
			if (weekday==0){
				if ([11,12,14,15,18,5].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([11,13,18,6,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([11,12,2,4,7,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([11,12,16,5,6,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([10,13,6,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([10,13,19,4,5,6,7].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([12,14,15,16,18,4,9].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='other'){
			if (weekday==0){
				if ([15,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([10,17,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([20,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([13,14,16,18,20,3,5,6].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([12,3,5,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([19,4].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([10,11,12,13,14,15,17,19,7,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='search'){
			if (weekday==0){
				if ([12,4,5,6,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([10,12,14,15,17,18,3,4,5,7,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([10,11,12,17,18,20,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([10,11,12,13,14,17,18,20,4,5,6,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([10,12,17,19,2,3,4,5,6,8,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([10,11,12,14,17,19,2,3,4,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([10,11,13,14,15,16,17,19,3,7,8,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='social'){
			if (weekday==0){
				if ([11,12,13,14,18,2,6,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([10,11,13,15,16,17,18,3,4,5,6,7,8,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([12,15,16,19,20,3,4,5,6,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([10,12,13,14,17,18,3,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([10,11,14,15,17,19,3,4,6,7,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([10,11,15,19,4,5,6,7,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([12,14,17,18,20,4,8].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
	}
	else if (state=='mi'){
		if (ref=='direct'){
			if (weekday==0){
				if ([11,13,15,16,18,3,4,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([11,12,17,20,4,5,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([11,13,17,18,20,4,5,7,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([10,12,13,14,15,7,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([11,13,14,15,16,4,5].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([12,17,18,2,3,4,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([12,13,14,15,19,5,6,9].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='external'){
			if (weekday==0){
				if ([10,14,3,6,7].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([10,12,13,15,7,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if (hour==11){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([13,14,15,3,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([11,3,4,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([13,20].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([11,13,18,7].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='other'){
			if (weekday==0){
				if ([11,13,3,5,6,7].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([11,13,5,6].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([10,13,7].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([13,18,20,7,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([19,5,6,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if (hour==10){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([13,16,6,7].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='search'){
			if (weekday==0){
				if ([10,11,13,5,6].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([11,13,7].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([10,12,13,15,16,5,6,7,8,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([13,18,20,7,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([14,15,17,19,20,5,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([10,20,4,6,7,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([10,13,17,20,5,6].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='social'){
			if (weekday==0){
				if ([11,12,13,15,17,18,2,6,7,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([11,12,13,16,18,20,5,6,7].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([12,13,16,17,18,20,3,5,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([10,20,4,8,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([10,12,17,19,4,5,6].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([11,13,14,20,5,7].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([10,12,17,18,19,2,6,7].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
	}
	else if (state=='not_usa'){
		if (ref=='direct'){
			if (weekday==0){
				if ([10,11,12,13,14,16,17,18,19,2,20,4,5,6,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([10,11,12,13,14,15,16,17,19,2,20,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([10,12,13,14,15,16,17,18,2,20,3,4,5,6,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([10,11,12,13,15,16,17,18,2,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([10,11,12,13,14,15,2,3,4,5,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([10,11,12,13,14,15,16,17,18,2,4,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([11,12,13,14,15,17,18,19,2,3,4,5,6,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='external'){
			if (weekday==0){
				if ([12,15,16,18,19,2,20,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([12,13,16,18,2,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([16,4,5,7,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([10,11,12,17,2,4,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([18,4,5,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([10,11,14,15,16,19,20,4,5,6,7,8,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([13,14,15,19,4,5,6,9].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='other'){
			if (weekday==0){
				if ([12,3,6].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([19,5,7,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([12,13,2,4,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([17,4,5,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([10,11,12,13,4,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if (hour==12){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([10,11,15,17,4,6,7].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='search'){
			if (weekday==0){
				if ([12,15,16,19,2,4,5,6,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([10,12,13,19,2,3,5,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([10,11,12,14,15,16,17,18,4,5,6,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([11,15,18,4,5,7,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([10,11,14,15,18,20,3,4,5,7,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([10,13,16,17,18,4,5,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([10,13,14,16,19,2,6,9].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='social'){
			if (weekday==0){
				if ([10,11,12,14,15,16,17,18,19,2,20,3,4,6,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([10,12,13,14,15,17,2,20,3,4,5,7,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([12,13,14,17,18,19,2,3,5,6,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([11,13,15,16,17,18,19,2,4,5,6,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([10,11,12,15,16,18,2,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([10,11,13,14,15,16,17,18,19,2,20,5,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([10,11,12,13,15,16,17,18,2,20,4,5,7,8,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
	}
	else if (state=='ny'){
		if (ref=='direct'){
			if (weekday==0){
				if ([10,11,12,13,14,15,16,17,18,19,2,20,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([10,11,12,13,14,15,16,17,18,19,20,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([10,11,12,13,14,15,16,18,19,2,20,3,4,5,6,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([10,11,12,13,14,15,16,17,18,19,20,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([10,11,12,13,14,15,16,17,18,19,2,20,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([10,11,12,13,14,15,17,18,19,2,20,3,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([10,11,12,13,14,15,16,17,18,19,2,20,3,4,5,6,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='external'){
			if (weekday==0){
				if ([10,11,13,14,15,16,19,2,20,3,4,5,6,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([11,12,13,14,16,17,18,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([10,11,12,13,14,15,18,19,2,20,3,4,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([10,13,14,16,19,20,3,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([10,11,12,13,14,15,16,17,18,19,4,5,6,8,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([11,12,14,16,17,19,20,5,6,8,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([11,12,13,14,16,17,20,4,6,7,8].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='other'){
			if (weekday==0){
				if ([16,18,20,4,5,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([11,14,15,3,4].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([10,12,17,18,19,2,20,3,4,6,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([10,11,12,18,20,4,5,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([12,13,14,4].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([10,18,4,5,6].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([10,12,14,17,19,8].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='search'){
			if (weekday==0){
				if ([10,11,12,13,14,15,16,17,18,19,2,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([10,11,12,13,14,15,16,17,18,19,2,20,5,6,7,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([10,11,12,13,14,16,17,18,19,3,4,5,6,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([10,11,12,13,14,15,16,17,18,19,20,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([10,11,12,13,14,15,16,17,18,19,2,20,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([10,11,12,13,14,15,16,17,18,19,20,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([10,11,12,13,14,15,16,17,18,19,2,20,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='social'){
			if (weekday==0){
				if ([10,11,12,13,14,15,16,17,18,19,20,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([10,11,12,13,14,15,16,17,18,19,2,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([10,11,12,13,14,16,17,18,19,2,20,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([10,11,12,13,14,15,16,17,18,19,2,20,4,5,6,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([10,11,12,13,14,15,16,17,18,19,2,20,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([10,11,12,13,14,15,16,17,18,19,20,3,4,5,6,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([10,11,12,13,14,15,16,17,18,19,2,20,3,4,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
	}
	else if (state=='other'){
		if (ref=='direct'){
			if (weekday==0){
				if ([10,11,12,13,14,15,16,17,18,19,2,20,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([10,11,12,13,14,15,16,17,18,19,20,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([10,11,12,13,14,15,16,17,18,20,3,4,5,6,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([10,11,12,13,14,15,16,17,18,19,2,20,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([10,11,12,13,14,15,16,17,18,19,2,20,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([10,11,12,13,14,15,16,17,18,19,2,20,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([10,11,12,13,14,15,16,17,18,19,2,20,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='external'){
			if (weekday==0){
				if ([10,11,12,13,15,17,18,19,2,20,3,4,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([10,11,13,15,17,18,19,3,4,6,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([10,11,12,13,14,15,16,20,4,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([10,11,12,13,14,16,2,20,3,4,6,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([10,12,13,14,15,16,17,20,4,5,6,7,8,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([10,11,12,13,14,15,16,18,20,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([13,14,17,18,2,3,4,5,6,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='other'){
			if (weekday==0){
				if ([10,12,14,16,20,3,4,7,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([10,15,17,18,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([10,14,15,16,17,2,20,4,6,7,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([11,14,16,2,4,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([10,11,12,13,14,16,19,2,3,4,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([10,11,12,13,14,15,19,4,5,7].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([13,16,6,7].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='search'){
			if (weekday==0){
				if ([10,11,12,13,14,15,16,17,18,19,2,20,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([10,11,12,13,14,15,16,17,18,19,2,20,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([10,11,12,13,14,15,16,17,18,19,2,20,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([10,11,12,13,14,15,16,17,18,19,20,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([10,11,12,13,14,15,16,17,18,19,2,20,3,4,5,6,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([10,11,12,13,14,15,16,17,18,19,2,20,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([10,11,12,13,14,15,16,17,18,19,2,20,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='social'){
			if (weekday==0){
				if ([10,11,12,13,14,15,16,17,18,19,2,20,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([10,11,12,13,14,15,16,17,18,19,2,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([10,11,12,13,14,15,16,17,18,19,2,20,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([10,11,12,13,14,15,16,17,18,19,2,20,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([10,11,12,13,14,15,16,17,18,19,2,20,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([10,11,12,13,14,15,16,17,18,19,2,20,3,4,5,6,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([10,11,12,13,14,15,16,17,18,19,2,20,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
	}
	else if (state=='tx'){
		if (ref=='direct'){
			if (weekday==0){
				if ([10,11,12,13,14,16,17,19,20,4,5,6,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([10,15,16,17,18,4,5,6,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([10,12,13,14,15,16,2,5,8,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([10,11,12,13,15,16,19,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([10,11,12,14,15,16,17,19,3,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([11,12,13,14,16,3,5,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([11,12,14,15,18,19,3,4,6,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='external'){
			if (weekday==0){
				if ([11,12,16,3,4,5,7,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([11,12,13,14,16,4,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([10,11,13,15,18,3,5,6,8,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([12,13,20,7,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([10,12,13].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([10,14,19,20,6,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([10,11,3,7,8].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='other'){
			if (weekday==0){
				if ([14,16].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([10,14,17,18,19,4,6,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([12,13,14].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if (hour==14){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([4,5,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([13,14].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([5,6].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='search'){
			if (weekday==0){
				if ([10,12,14,15,16,17,20,5,6,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([10,12,14,15,17,18,19,4,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([11,12,14,15,16,17,18,19,5,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([10,12,13,14,17,18,5,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([12,17,3,4,5,6,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([10,11,12,13,14,16,17,2,20,6,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([10,12,13,15,16,18,19,4,5,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='social'){
			if (weekday==0){
				if ([11,13,14,15,16,18,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([10,14,15,16,17,18,19,4,5,6,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([10,11,13,14,16,17,18,19,20,4,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([10,11,12,14,16,18,19,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([10,11,12,17,18,19,20,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([10,11,12,15,18,4,5,7,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([10,11,13,14,18,19,20,3,6,9].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
	}
	else if (state=='wa'){
		if (ref=='direct'){
			if (weekday==0){
				if ([10,12,13,20,6,7,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([10,12,13,15,17,19,2,3,5,6,7,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([10,11,12,13,16,17,18,2,6,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([10,11,12,13,14,15,17,5,6,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([11,12,2,3,4,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([10,11,12,14,17,18,4,5,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([10,14,15,17,6,8].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='external'){
			if (weekday==0){
				if ([10,11,12,20,7,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([12,13,3,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([2,20,4,5,6,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([10,11,17,6].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([11,12,14,15,18,4,6,7,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([11,12,20,4,7].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([15,17,18,19,5,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='other'){
			if (weekday==0){
				if ([11,13,16,20,3,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([11,15,16,18,4,6,7,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([11,4,6,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([11,13,16,17,19,7,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([11,12,13,16,3,4,5,7,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([11,13,19,7].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([11,6,8].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='search'){
			if (weekday==0){
				if ([10,11,13,16,20,5,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([12,13,16,18,20,6].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([10,11,12,16,3,4,6,8,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([10,11,12,13,15,16,17,19,6,7,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([11,12,13,14,19,2,6,7,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([10,12,13,14,19,7,9].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([10,11,12,16,17,5,6].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
		else if (ref=='social'){
			if (weekday==0){
				if ([10,14,15,16,17,2,4,5,6,7,8,9,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==1){
				if ([10,11,14,15,18,19,4,5,6,7,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==2){
				if ([11,12,13,14,17,19,2,3,5,6,7,8].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==3){
				if ([11,12,13,14,16,17,3,7,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==4){
				if ([11,12,13,14,16,17,18,19,20,3,5,6,7,8,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==5){
				if ([11,13,4,8,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
			else if (weekday==6){
				if ([11,12,18,3,4,6,7,0,1,21,22,23].includes(hour)){
					TARGET=TRUE;
				}
			}
		}
	}
}
