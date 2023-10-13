import { animate, style, transition, trigger } from '@angular/animations';
import {
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  OnDestroy,
  OnInit,
  TemplateRef,
  TrackByFunction,
  ViewChild,
  inject
} from '@angular/core';
import { PartialMessage, Timestamp } from '@bufbuild/protobuf';
import { GetWorkingStaffResponse, PlannedShift, Profile, WorkShift } from '@tkd/apis';
import { CreateOverwriteRequest, Overwrite } from '@tkd/apis/gen/es/tkd/pbx3cx/v1/calllog_pb';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzModalService } from 'ng-zorro-antd/modal';
import {
  BehaviorSubject,
  Subject,
  combineLatest,
  forkJoin,
  interval
} from 'rxjs';
import {
  debounceTime,
  filter,
  map, startWith,
  switchMap,
  takeUntil
} from 'rxjs/operators';
import {
  ConfigAPI,
  QuickRosterOverwrite,
  UserService
} from 'src/app/api';
import { CALL_SERVICE, ROSTER_SERVICE, WORK_SHIFT_SERVICE } from 'src/app/api/connect_clients';
import { ProfileService } from 'src/app/services/profile.service';
import { HeaderTitleService } from 'src/app/shared/header-title';
import { extractErrorMessage } from 'src/app/utils';

@Component({
  templateUrl: './overwrite.component.html',
  styleUrls: ['./overwrite.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  animations: [
    trigger('scaleInOut', [
      transition(':enter', [
        style({ maxHeight: '0', opacity: 0 }),
        animate('200ms', style({ maxHeight: '1000px', opacity: 1 })),
      ]),
      transition(':leave', [
        style({ maxHeight: '1000px', opacity: 1 }),
        animate('200ms', style({ maxHeight: 0, opacity: 0 })),
      ]),
    ]),
  ],
})
export class OnCallOverwritePageComponent implements OnInit, OnDestroy {
  private destroy$ = new Subject<void>();
  private checkOverlapping$ = new Subject<{ to: Date; from: Date }>();
  private reloadToday$ = new BehaviorSubject<void>(undefined);

  @ViewChild('confirmDeleteCurrentOverwrite', {
    read: TemplateRef,
    static: true,
  })
  confirmDeleteCurrentOverwriteTemplate: TemplateRef<any> | null = null;

  /** A list of overwrites that overlap with the current one */
  overlapping: Overwrite[] = [];

  /** AvailableShifts holds the available shifts at the selected date */
  availableShifts: (PlannedShift & {definition: WorkShift})[] = [];

  /** A list of users that are preferable used as overwrites */
  preferredUsers: Profile[] = [];

  /** A list of other users that can be used as overwrites */
  allUsers: Profile[] = [];

  /** Whether or not all users should be shown */
  showAllUsers = false;

  /** The id of the user that has been selected */
  selectedUser = '';

  /** The selected quick roster overwrite */
  selectedQuickTargetNumber = '';

  /** The time at which the overwrite should be effective when overwriteTarget == 'custom' */
  customFrom: Date | null = null;

  /** The time until the overwrite should be effective when overwriteTarget === 'custom' */
  customTo: Date | null = null;

  /** The currently active overwrite, if any */
  currentOverwrite: Overwrite | null = null;

  /** The current doctor on duty */
  currentRoster: GetWorkingStaffResponse | null = null;

  /** The type of overwrite that is being created */
  overwriteTarget: string | 'custom' = '';

  /** A list of configured quick-settings  */
  quickSettings: QuickRosterOverwrite[];

  /** Whether or not direct phone-number overwrites are allowed */
  allowPhone = false;

  /** The custom phone number entered by the user if allowPhone is true */
  customPhoneNumber = '';

  /** Whether or not we're in "firstLoad" mode and should hide the overlapping warning */
  firstLoad = true;

  /** Evaluates to true if everything is ready to create a new roster overwrite */
  get valid(): boolean {
    // FIXME
    return true;
  }

  /** @private template-only - Returns true if d is before the current roster date */
  isBeforeRosterDate = (d: Date): boolean => {
    if (!d) {
      return false;
    }
    const now = new Date();
    let rosterDate = new Date(now.getFullYear(), now.getMonth(), now.getDate());

    return d.getTime() < rosterDate.getTime();
  };

  /** @private template-only - decided whether or not startValue should be displayed as disabled. */
  disabledStartDate = (startValue: Date): boolean => {
    // FIXME
    return false
  };

  /** @private template-only - decided whether or not endValue should be displayed as disabled. */
  disabledEndDate = (endValue: Date): boolean => {
    // FIXME
    return false
  };

  /** TrackBy function for user profiles */
  readonly trackProfile: TrackByFunction<Profile> = (
    _: number,
    p: Profile
  ) => p.user.id;

  /** TrackBy function for quick settings */
  readonly trackQuickSetting: TrackByFunction<QuickRosterOverwrite> = (
    _: number,
    s: QuickRosterOverwrite
  ) => s.TargetNumber;

  private rosterService = inject(ROSTER_SERVICE);
  private workShiftService = inject(WORK_SHIFT_SERVICE)
  private callService = inject(CALL_SERVICE);

  constructor(
    private userService: UserService,
    private account: ProfileService,
    private config: ConfigAPI,
    private cdr: ChangeDetectorRef,
    private modal: NzModalService,
    private nzMessage: NzMessageService,
    private header: HeaderTitleService
  ) {}

  onDateChange(humanInteraction = true) {
    if (humanInteraction) {
      this.firstLoad = false;
    }

    let from: Date;
    let to: Date;

    if (this.overwriteTarget === 'custom') {
      // FIXME(ppacher): we need to reload the available shifts now
      from = this.customFrom;
      to = this.customTo;
    } else {
      from = new Date(this.customFrom)
      to = new Date(from.getFullYear(), from.getMonth(), from.getDate() + 1)
    }

    // customTo is not set so better bail out if one of them is
    // currently invalid/unset
    if (!from || !to) {
      return;
    }

    Promise.all([
      this.rosterService
        .getWorkingStaff({
          onCall: true,
          time: Timestamp.fromDate(from),
        }),
      this.workShiftService.listWorkShifts({})
    ])
      .then(([response, shifts]) => {
        this.availableShifts = response.currentShifts.map(shift => {
          Object.defineProperty(shift, 'definition', {
            get: () => {
              return shifts.workShifts.find(ws => ws.id === shift.workShiftId)
            }
          })

          return shift as any
        });

          let set = new Map<string, Profile>();
          this.availableShifts.forEach(shift =>
            shift.assignedUserIds.forEach(staff => {
              set.set(staff, this.userService.byId(staff))
            }))

          // actually get the preferred users.
          this.preferredUsers = Array.from(set.values())
            .sort((a, b) => {
              if (a.user.username > b.user.username) {
                return 1;
              }

              if (a.user.username < b.user.username) {
                return -1
              }
              return 0
          })

        // we just changed the date so the currently selected shift to overwrite
        // might not be available anymore. Better update the overwriteTarget
        // to either the first shift available or set it to "custom".
        if (this.overwriteTarget !== 'custom') {
          if (this.availableShifts.length > 0) {
            this.overwriteTarget = this.availableShifts[0].workShiftId;
          } else {
            this.overwriteTarget = 'custom';
          }
        }

        this.cdr.markForCheck();
      })

    this.checkOverlapping$.next({ from, to });
  }

  /** @private template-only - select the user that should be used for the new overwrite */
  selectUser(u: string) {
    this.selectedUser = u;
    this.selectedQuickTargetNumber = '';
    this.customPhoneNumber = '';
  }

  /** @private template-only - selects the quick roster overwrite that should be used */
  selectQuickSetting(s: QuickRosterOverwrite) {
    this.selectedUser = '';
    this.selectedQuickTargetNumber = s.TargetNumber;
    this.customPhoneNumber = '';
  }

  /** @private template-only - called when the user enters a customer phone number */
  onCustomPhoneNumberChange() {
    this.selectedUser = '';
    this.selectedQuickTargetNumber = '';
  }

  /** @private template-only - creates a new roster overwrite */
  createOverwrite() {
    if (!this.valid) {
      return;
    }

    const { from, to } = this.getTimeBoundary();

    const req: PartialMessage<CreateOverwriteRequest> = {
      from: Timestamp.fromDate(from),
      to: Timestamp.fromDate(to)
    }

    let target: string = '';

    if (!!this.selectedQuickTargetNumber) {
      const s = this.quickSettings.find(
        (q) => q.TargetNumber === this.selectedQuickTargetNumber
      );
      if (!s) {
        this.selectedQuickTargetNumber = '';
        // TODO(ppacher): display error message
        return;
      }
      req.transferTarget = {
        case: 'custom',
        value: {
          displayName: s.DisplayName || s.TargetNumber,
          transferTarget: s.TargetNumber
        }
      }

      target = s.DisplayName
    } else
    if (!!this.customPhoneNumber) {
      req.transferTarget = {
        case: 'custom',
        value: {
          displayName: this.customPhoneNumber,
          transferTarget: this.customPhoneNumber,
        }
      }

      target = this.customPhoneNumber
    } else
    if (!!this.selectedUser) {
      req.transferTarget = {
        case: 'userId',
        value: this.selectedUser
      }
      const profile = this.userService.byId(this.selectedUser);
      target = profile.user.displayName || profile.user.username;
    }


    this.callService.createOverwrite(req)
      .then(() => {
        this.nzMessage.success(`Telefon erfolgreich auf ${target} umgeleitet.`);
        this.reloadToday$.next();
        this.firstLoad = true;
        this.overwriteTarget = '';
        this.checkOverlapping$.next({ from, to });
      })
      .catch((err) => {
        this.firstLoad = false;
        this.nzMessage.error(
          extractErrorMessage(err, 'Telefon konnte nicht umgeleitet werden')
        );
        this.cdr.markForCheck();
      })
  }

  /** @private template-only - deletes the current overwrite */
  deleteCurrentOverwrite() {
    if (!this.currentOverwrite || !this.confirmDeleteCurrentOverwriteTemplate) {
      return;
    }

    this.modal.confirm({
      nzTitle: 'Umleitung zurücksetzten?',
      nzContent: this.confirmDeleteCurrentOverwriteTemplate,
      nzCancelText: 'Nein',
      nzOkText: 'Ja, löschen',
      nzOnOk: () => {
        this.deleteOverwrite(this.currentOverwrite);
      },
    });
  }

  get actualBoundary(): { to: Date, from: Date } {
    return this.getTimeBoundary();
  }

  /** @private template-only - deletes a overwrite by ID */
  deleteOverwrite(ov: Overwrite) {
    if (!ov || !ov.id) {
      return;
    }

    this.callService.deleteOverwrite({selector: {
      case: 'overwriteId',
      value: ov.id,
    }})
    .then(() => {
        this.nzMessage.success('Eintrag wurde erfolgreich gelöscht');
        this.onDateChange();
        this.reloadToday$.next();
    })
    .catch(err => {
        this.nzMessage.error(
          extractErrorMessage(err, 'Eintrag konnte nicht gelöscht werden')
        )
    })
  }

  ngOnInit() {
    this.destroy$ = new Subject();

    this.header.set(
      'Telefon Umleitungen',
      'Überschreibe den Dienstplan und wähle ein neues Ziel für Anrufe außerhalb der Öffnungszeiten'
    );

    // load and watch preferred and other allowed users.
    const rosterConfig$ = this.config.change;
    combineLatest([rosterConfig$, this.userService.users])
      .pipe(
        takeUntil(this.destroy$)
      )
      .subscribe(([c, users]) => {
        let currentUserRoles = new Set<string>();
        this.account.snapshot?.roles.forEach(role => {
          currentUserRoles.add(role.id)
        })

        this.quickSettings = (c.QuickRosterOverwrite || []).filter(setting => {
          return !setting.RequiresRole || setting.RequiresRole.length === 0 || setting.RequiresRole.some(role => currentUserRoles.has(role));
        });

        this.allUsers = users;
        this.allowPhone = c.Roster?.AllowPhoneNumberOverwrite || false;
        this.cdr.markForCheck();
      });

    // load and watch the currently active overwrite for this day
    combineLatest([interval(5000).pipe(startWith(-1)), this.reloadToday$])
      .pipe(
        map(() => new Date()),
        switchMap(d => forkJoin({
          currentOverwrite: this.callService.getOverwrite({selector: {
            case: 'activeAt',
            value: Timestamp.fromDate(d),
          }}),
          currentlyActiveRoster: this.rosterService
              .getWorkingStaff({
                onCall: true,
                time: Timestamp.fromDate(d),
              })
        })),
        takeUntil(this.destroy$),
      )
      .subscribe(result => {
        this.currentOverwrite = null;

        if (result.currentOverwrite.overwrites.length) {
          this.currentOverwrite = result.currentOverwrite.overwrites[0];
        }
        this.currentRoster = result.currentlyActiveRoster;

        this.cdr.markForCheck();
      });

    // check for overlapping overwrites.
    this.checkOverlapping$
      .pipe(
        takeUntil(this.destroy$),
        debounceTime(100),
        filter(() => this.overwriteTarget != ''),
        switchMap(({from, to}) => {
          return forkJoin({
            overlapping: this.callService
              .getOverwrite({
                selector: {
                  case: 'timeRange',
                  value: {
                    from: Timestamp.fromDate(from),
                    to: Timestamp.fromDate(to),
                  }
                }
              })
          });
        })
      )
      .subscribe((state) => {
        this.overlapping = state.overlapping.overwrites
        this.cdr.markForCheck();
      });

    this.customFrom = new Date();
    this.onDateChange(false)
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
    this.reloadToday$.complete();
  }

  /**
   * Returns a list of user profiles that match the given term.
   * If term is an empty string, all preferred users are returned.
   *
   * @param term The search term to filter users
   */
  filterUsers(term: string): Profile[] {
    if (term === '') {
      return this.preferredUsers;
    }

    const result: Profile[] = [];
    const matchUser = (u: Profile) => {
      if (u.user.username.includes(term) || u.user.displayName.includes(term)) {
        result.push(u);
      }
    };

    this.preferredUsers.forEach(matchUser);
    this.allUsers.forEach(matchUser);
    return result;
  }

  private getTimeBoundary(): { from: Date; to: Date } {
    if (this.overwriteTarget === 'custom') {
      return {
        from: this.customFrom,
        to: this.customTo
      }
    }

    const shift = this.availableShifts.find(s => s.workShiftId === this.overwriteTarget);
    if (!shift) {
      debugger;

      return
    }

    return {
      from: shift.from.toDate(),
      to: shift.to.toDate()
    }
  }
}
