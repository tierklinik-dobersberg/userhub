import { ChangeDetectionStrategy, Component, computed, effect, model, output } from "@angular/core";
import { FormsModule } from "@angular/forms";
import { BrnSelectModule } from "@spartan-ng/ui-select-brain";
import { BrnSeparatorModule } from '@spartan-ng/ui-separator-brain';
import { BrnSheetModule } from "@spartan-ng/ui-sheet-brain";
import { injectComputedFilterSheetSide, injectCurrentProfile, injectUserProfiles } from "@tierklinik-dobersberg/angular/behaviors";
import { HlmButtonDirective } from "@tierklinik-dobersberg/angular/button";
import { HlmIconModule, provideIcons } from "@tierklinik-dobersberg/angular/icon";
import { HlmInputModule } from "@tierklinik-dobersberg/angular/input";
import { HlmLabelModule } from "@tierklinik-dobersberg/angular/label";
import { HlmSelectModule } from "@tierklinik-dobersberg/angular/select";
import { HlmSeparatorModule } from "@tierklinik-dobersberg/angular/separator";
import { HlmSheetModule } from "@tierklinik-dobersberg/angular/sheet";
import { OffTimeEntry } from "@tierklinik-dobersberg/apis";
import { AppAvatarComponent } from "src/app/components/avatar";
import { SelectUserValueComponent } from "src/app/components/select-user-value";

export type StateFilter = 'all' | 'new' | 'approved' | 'rejected';

export interface OffTimeFilter {
    // Show off-time entries for the specified users
    userIds?: string[];
    
    // Filter off-time entries by state.
    state: StateFilter;
    
    // Filter off-time entries that start or end after a given date
    from?: Date;
    
    // Filter off-time entries that start or end before a given date.
    to?: Date;
}

export function filterOffTimeEntries(entries: OffTimeEntry[], filter: OffTimeFilter): OffTimeEntry[] {
    let userIdSet: Set<string> | null = null;
    if (filter.userIds && filter.userIds.length > 0) {
        userIdSet = new Set(filter.userIds);
    } 

    return entries
        .filter(e => {
            if (userIdSet) {
                if (!userIdSet.has(e.requestorId)) {
                    return false;
                }
            }
            
            switch (filter.state) {
                case 'all':
                    break;
                
                case 'new':
                    if (e.approval) {
                        return false;
                    }
                    
                    break;

                case 'approved':
                    if (!e.approval || !e.approval.approved) {
                        return false
                    }

                    break;
                    
                case 'rejected':
                    if (!e.approval || e.approval.approved ) {
                        return false;
                    }
                    
                    break;
            }

            const from = e.from.toDate().getTime();
            const to = e.to.toDate().getTime();

            if (filter.from) {
                if (to < filter.from.getTime()) {
                    return false;
                }
            }
            
            if (filter.to) {
                if (from > filter.to.getTime()) {
                    return false;
                }
            }

            return true;
        })
}

@Component({
    selector: 'app-offtime-filter-sheet',
    standalone: true,
    imports: [
        HlmSheetModule,
        BrnSheetModule,
        HlmSelectModule,
        BrnSelectModule,
        HlmButtonDirective,
        HlmIconModule,
        HlmInputModule,
        HlmLabelModule,
        FormsModule,
        AppAvatarComponent,
        BrnSeparatorModule,
        HlmSeparatorModule,
        SelectUserValueComponent,
    ],
    templateUrl: './offtime-filter.component.html',
    changeDetection: ChangeDetectionStrategy.OnPush,
    providers: [
        ...provideIcons({})
    ]
})
export class AppOffTimeFilterSheetComponent {
    protected readonly profiles = injectUserProfiles(); 
    protected readonly currentUser = injectCurrentProfile();
    
    protected readonly userIds = model<string[]>([]);
    protected readonly state = model<StateFilter>('all');
    protected readonly from = model<string|null>(null)
    protected readonly to = model<string|null>(null);
    protected readonly sheetSide = injectComputedFilterSheetSide();
    protected readonly _computedFilterButtonVariant = computed(() => {
        const userIds = this.userIds();
        const state = this.state();
        const from = this.from();
        const to = this.to();
        const current = this.currentUser();

        if (from || to || state !== 'all') {
            return 'secondary'
        }
        
        if (userIds.length != 1 || userIds[0] !== current.user.id) {
            return 'secondary';
        }
        
        return 'outline';
    })
    
    public readonly filter = output<OffTimeFilter>();
    
    constructor() {
        // We default to only show off-time entries for the logged-in user.
        // Once we got the profile we update the userIds signal once.
        const effectRef = effect(() => {
            const current = this.currentUser();
            const userIds = this.userIds();

            if (!current) {
                return;
            }
            
            if (userIds.length === 0) {
               this.userIds.set([current.user!.id]) 
               effectRef.destroy();
            } 
            
            this.apply()
        }, { allowSignalWrites: true });
    }

    protected apply() {
        const userIds = this.userIds();
        const state = this.state();
        const from = this.from();
        const to = this.to();

        const filter: OffTimeFilter = {
            state: state,
            userIds: userIds,
        }
        
        if (from) {
            filter.from = new Date(from);
        }
        
        if (to) {
            filter.to = new Date(to);
        }
        
        this.filter.emit(filter);
    }
    
    protected reset() {
        this.userIds.set([this.currentUser().user.id])
        this.from.set(null)
        this.to.set(null);
        this.state.set('all')
        
        this.apply();
    }
}